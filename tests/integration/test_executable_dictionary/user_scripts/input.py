#!/usr/bin/python3

import os
import signal
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        updated_line = line.replace("\n", "")
        print(updated_line + "\t" + "Key " + updated_line, end="\n")
        sys.stdout.flush()
