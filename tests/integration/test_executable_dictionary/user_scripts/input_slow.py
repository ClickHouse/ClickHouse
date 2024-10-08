#!/usr/bin/python3

import os
import signal
import sys
import time

if __name__ == "__main__":
    for line in sys.stdin:
        time.sleep(5)
        updated_line = line.replace("\n", "")
        print(updated_line + "\t" + "Key " + updated_line, end="\n")
        sys.stdout.flush()
