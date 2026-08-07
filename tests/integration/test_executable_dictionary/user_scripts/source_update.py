#!/usr/bin/python3

import sys

if __name__ == "__main__":
    update_field_value = 0

    if len(sys.argv) >= 2:
        update_field_value = int(sys.argv[1])

    print("1" + "\t" + "Value " + str(update_field_value) + " 1", end="\n")
    sys.stdout.flush()
