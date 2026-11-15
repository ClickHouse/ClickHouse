#!/usr/bin/python3

import json
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        value = json.loads(line)
        first_arg = int(value["c1"])
        second_arg = int(value["c2"])
        result = {"result_name": first_arg + second_arg}
        print(json.dumps(result), end="\n")
        sys.stdout.flush()
