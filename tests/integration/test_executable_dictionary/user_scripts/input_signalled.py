#!/usr/bin/python3

import sys
import os
import signal
import time

if __name__ == "__main__":
    for line in sys.stdin:
        os.signal(os.getpid(), signal.SIGTERM)
        updated_line = line.replace("\n", "")
        print(updated_line + "\t" + "Key " + updated_line, end="\n")
        sys.stdout.flush()
