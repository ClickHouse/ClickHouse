#!/usr/bin/python3

import sys
import os
import signal
import time

if __name__ == "__main__":
    for line in sys.stdin:
        time.sleep(5)
        print("Key " + line, end="")
        sys.stdout.flush()
