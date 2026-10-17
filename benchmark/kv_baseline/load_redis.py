#!/usr/bin/env python3

import argparse
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load TSV key-value data into Redis. Requires redis-py: python3 -m pip install redis.")
    parser.add_argument("--input", required=True, help="Input TSV path.")
    parser.add_argument("--host", default="127.0.0.1", help="Redis host.")
    parser.add_argument("--port", type=int, default=6379, help="Redis port.")
    parser.add_argument("--db", type=int, default=0, help="Redis database number.")
    parser.add_argument("--pipeline-size", type=int, default=1000, help="Commands per pipeline execute.")
    parser.add_argument("--flushdb", action="store_true", help="Flush selected Redis database before loading.")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.pipeline_size <= 0:
        raise SystemExit("--pipeline-size must be positive")

    try:
        import redis
    except ImportError:
        raise SystemExit("redis-py is required. Install it with: python3 -m pip install redis")

    client = redis.Redis(host=args.host, port=args.port, db=args.db)
    try:
        client.ping()
    except redis.RedisError as exc:
        raise SystemExit(f"Cannot connect to Redis: {exc}")

    if args.flushdb:
        print(f"Flushing Redis db {args.db}")
        client.flushdb()

    loaded = 0
    pipe = client.pipeline(transaction=False)

    with open(args.input, "r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                key, value = line.split("\t", 1)
            except ValueError:
                print(f"Skipping malformed line {line_number}", file=sys.stderr)
                continue

            pipe.set(key, value)
            loaded += 1

            if loaded % args.pipeline_size == 0:
                pipe.execute()
                print(f"Loaded {loaded} rows")

    pipe.execute()
    print(f"Loaded {loaded} rows total")


if __name__ == "__main__":
    main()
