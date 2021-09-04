#!/bin/python3

import sys
import fileinput
import re


ARG_SPLIT_RE = re.compile('\W+')
ALPHABETIC_KEY_RE = re.compile('[A-Za-z:]')

def parse(command_str): 
    args = ARG_SPLIT_RE.split(command_str)
    return args

class Validate:
    def key_name(name):
        if not ALPHABETIC_KEY_RE.search(name):
            print("Error: invalid key. Non-alphabetic characters detected in key '{}'".format(name))
            return False
        else: return True

    def arg_count(command_str, count, expected):
        if count != expected :
            print( "Error: {} expected argument count is {}. {} provided.".format(
                command_str,
                expected,
                count))
            return False
        else: return True

class Database:
    def __init__(self):
        self.data = {}
        self.transactions = [self.data]
        self.vtable = {
            "SET": self.set,
            "GET": self.get,
            "UNSET": self.unset,
            "NUMEQUALTO": self.numequalto,
            "END": self.end,
            "BEGIN": self.begin,
            "ROLLBACK": self.rollback,
            "COMMIT" : self.commit
        }

    def current_transaction(self) :
        return self.transactions[len(self.transactions) - 1]

    # Commands
    def set(self, args) :
        if( Validate.arg_count("SET", len(args), 2) and
            Validate.key_name(args[0])) :
            self.current_transaction()[args[0]] = int(args[1])


    def get(self, args) :
        result = None
        if (Validate.arg_count("GET", len(args), 1) and
            Validate.key_name(args[0])): 
            for transaction in reversed(self.transactions):
                result = transaction.get(args[0], None)
                if result is not None: break
        if result is None : return "NULL"
        else: return result

        
        
    def unset(self, args) :
        if (Validate.arg_count("UNSET", len(args), 1) and
            Validate.key_name(args[0])): 
            self.set([args[0], None])

    #TODO: current time is O(n), needs improvement
    def numequalto(self, args) :
        count = 0
        if Validate.arg_count("NUMEQUALTO", len(args), 1):
            for transaction in reversed(self.transactions):
                val = int(args[0])
                for v in transaction.values():
                    if v == val: count += 1
        print(count)
            

    def end(self, args) :
        exit()

    def begin(self, args) :
        self.transactions.append({})

    def rollback(self, args) :
        if len(self.transactions) == 1:
            print("NO TRANSACTION")
        else: 
            # leave at least one transaction open
            self.transactions = [self.transactions[0]] 

    def commit_recurse(self):
        if len(self.transactions) != 1:
            transaction = self.transactions.pop()
            parent_transaction = self.current_transaction()
            for k,v in transaction.items():
                parent_transaction[k] = v
            self.commit_recurse()

    def commit(self, args) :
        if len(self.transactions) == 1:
            return "NO TRANSACTION"
        else: 
            self.commit_recurse()

    def exec(self, command_str) :
        parsed = parse(command_str)
        # Check for EOF trailing empty string, and trim if needed
        if parsed[len(parsed) - 1] == '':
            parsed = parsed[:-1]
        command = self.vtable.get(
            parsed[0].upper(), 
            None
        ) 
        if command is None:
            print("Error: unrecognized command '{}'".format(parsed[0]))
        else:
            output = command(parsed[1:])
            if output != None: print(output)

def test_assert(test, result, expected):
    if result != expected:
        print("{}: unexpected test result '{}'. Expecting: {}".format(test, result, expected))
    else:
        print("{}: pass".format(test))

def set_test():
    db = Database()
    db.set(['x', 1])
    result = db.current_transaction()['x']
    test_assert("set_test", result, 1)

def get_test():
    db = Database()
    db.current_transaction()['x'] = 1
    result = db.get(['x'])
    test_assert("get_test", result, 1)

def begin_test():
    db = Database()
    # there should be at least one default transaction
    test_assert("begin_test-1", len(db.transactions), 1)
    db.exec("BEGIN")
    test_assert("begin_test-2", len(db.transactions), 2)
    db.exec("BEGIN")
    test_assert("begin_test-3", len(db.transactions), 3)
    
def rollback_test():
    db = Database()
    db.exec("SET x 0")
    db.exec("BEGIN")
    db.exec("SET x 1")
    db.exec("ROLLBACK")
    result = db.get(['x'])
    test_assert("rollback_test", len(db.transactions), 1)
    test_assert("rollback_test", result, 0)

def commit_test():
    db = Database()
    db.exec("SET x 1")
    db.exec("BEGIN")
    db.exec("SET x 2")
    db.exec("COMMIT")
    test_assert("commit_test", db.current_transaction()['x'], 2)

def transaction_mask_value_test():
    db = Database()
    db.exec("SET x 1")
    result = db.get(['x'])
    test_assert("transaction_mask_value_test-1", result, 1);
    db.exec("BEGIN")
    db.exec("SET x 2")
    result = db.get(['x'])
    test_assert("transaction_mask_value_test-2", result, 2);

def tests() :
    db = Database()
    set_test()
    get_test()
    begin_test()
    rollback_test()
    commit_test()
    transaction_mask_value_test()

def main() :
    if len(sys.argv) == 2 and sys.argv[1] == "test": tests()
    else:
        db = Database();
        for line in sys.stdin:
            db.exec(line)

main()

