#!/usr/bin/env python3

import argparse
import random
import string
from pathlib import Path


ALPHABET = string.ascii_letters + string.digits


def parse_args():
    parser = argparse.ArgumentParser(description="Generate deterministic TSV key-value data.")
    parser.add_argument("--rows", type=int, required=True, help="Number of rows to generate.")
    parser.add_argument("--value-size", type=int, required=True, help="Value size in bytes/chars.")
    parser.add_argument("--output", required=True, help="Output TSV path.")
    parser.add_argument("--keys-output", help="Optional output path with one key per line.")
    parser.add_argument("--seed", type=int, default=42, help="Pseudo-random seed.")
    return parser.parse_args()


def make_value(rng, size):
    return "".join(rng.choice(ALPHABET) for _ in range(size))


def main():
    args = parse_args()
    if args.rows < 0:
        raise SystemExit("--rows must be non-negative")
    if args.value_size < 0:
        raise SystemExit("--value-size must be non-negative")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    keys_output = Path(args.keys_output) if args.keys_output else None
    if keys_output:
        keys_output.parent.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)

    with output.open("w", encoding="utf-8", newline="\n") as data_file:
        if keys_output:
            with keys_output.open("w", encoding="utf-8", newline="\n") as keys_file:
                for i in range(args.rows):
                    key = f"key_{i:09d}"
                    value = make_value(rng, args.value_size)
                    data_file.write(f"{key}\t{value}\n")
                    keys_file.write(f"{key}\n")
        else:
            for i in range(args.rows):
                key = f"key_{i:09d}"
                value = make_value(rng, args.value_size)
                data_file.write(f"{key}\t{value}\n")

    print(f"Wrote {args.rows} rows to {output}")
    if keys_output:
        print(f"Wrote {args.rows} keys to {keys_output}")


if __name__ == "__main__":
    main()
