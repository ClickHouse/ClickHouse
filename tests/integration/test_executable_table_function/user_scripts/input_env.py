#!/usr/bin/python3
import os
import sys

if __name__ == "__main__":
    env_vars = [
        os.environ.get("FOO_1", "(null)"),
        os.environ.get("FOO_2", "(null)"),
        os.environ.get("BAR_1", "(null)"),
        os.environ.get("BAR_2", "(null)"),
    ]

    for line in sys.stdin:
        print("Key " + " ".join(env_vars) + " " + line, end="")
        sys.stdout.flush()
