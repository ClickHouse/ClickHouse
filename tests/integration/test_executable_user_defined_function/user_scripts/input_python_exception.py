#!/usr/bin/python3

import sys

def process_data(value):
    """Function that raises exception with traceback"""
    # This will raise ZeroDivisionError with full traceback
    result = int(value) / 0
    return result

if __name__ == "__main__":
    for line in sys.stdin:
        try:
            value = line.strip()
            result = process_data(value)
            print(result)
            sys.stdout.flush()
        except Exception as e:
            # Print full exception with traceback to stderr
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            sys.exit(1)
