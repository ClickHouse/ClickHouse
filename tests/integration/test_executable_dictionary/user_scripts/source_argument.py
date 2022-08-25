#!/usr/bin/python3

import sys

if __name__ == "__main__":
    arg = int(sys.argv[1])

    print("1" + "\t" + "Value " + str(arg) + " 1", end="\n")
    print("2" + "\t" + "Value " + str(arg) + " 2", end="\n")
    print("3" + "\t" + "Value " + str(arg) + " 3", end="\n")

    sys.stdout.flush()
