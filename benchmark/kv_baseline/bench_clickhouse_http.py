#!/usr/bin/env python3

import argparse
import csv
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


CSV_FIELDS = [
    "system",
    "interface",
    "dataset_size",
    "value_size",
    "batch_size",
    "concurrency",
    "qps",
    "p50_ms",
    "p95_ms",
    "p99_ms",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark ClickHouse HTTP SQL point lookups.")
    parser.add_argument("--host", default="127.0.0.1", help="ClickHouse HTTP host.")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port.")
    parser.add_argument("--database", default="default", help="ClickHouse database.")
    parser.add_argument("--table", default="kv_baseline", help="ClickHouse table.")
    parser.add_argument("--keys-file", required=True, help="File with one key per line.")
    parser.add_argument("--duration", type=float, default=30, help="Benchmark duration in seconds.")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of worker threads.")
    parser.add_argument("--batch-size", type=int, default=1, help="Keys per request.")
    parser.add_argument("--output", help="Optional CSV output file. One row is appended.")
    parser.add_argument("--dataset-size", help="Dataset size label for CSV output.")
    parser.add_argument("--value-size", help="Value size label for CSV output.")
    return parser.parse_args()


def quote_identifier(identifier):
    return "`" + identifier.replace("`", "``") + "`"


def quote_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def percentile(sorted_values, percentile_value):
    if not sorted_values:
        return 0.0
    index = int((percentile_value / 100.0) * (len(sorted_values) - 1))
    return sorted_values[index]


def load_keys(path):
    with open(path, "r", encoding="utf-8") as keys_file:
        keys = [line.strip() for line in keys_file if line.strip()]
    if not keys:
        raise SystemExit("keys file is empty")
    return keys


def build_query(database, table, keys):
    table_name = f"{quote_identifier(database)}.{quote_identifier(table)}"
    if len(keys) == 1:
        return f"SELECT value FROM {table_name} WHERE key = {quote_string(keys[0])}"
    values = ", ".join(quote_string(key) for key in keys)
    return f"SELECT key, value FROM {table_name} WHERE key IN ({values})"


def execute_query(url, query):
    data = query.encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse HTTP error {exc.code}: {body}") from exc


def worker(worker_id, args, keys, url, deadline, latencies, errors):
    local_latencies = []
    key_count = len(keys)
    index = worker_id * args.batch_size

    while time.perf_counter() < deadline:
        batch = [keys[(index + offset) % key_count] for offset in range(args.batch_size)]
        index = (index + args.batch_size * args.concurrency) % key_count
        query = build_query(args.database, args.table, batch)

        start = time.perf_counter()
        try:
            execute_query(url, query)
        except Exception as exc:
            errors.append(str(exc))
            break
        local_latencies.append((time.perf_counter() - start) * 1000.0)

    latencies.extend(local_latencies)


def append_csv(path, row):
    file_exists = os.path.exists(path)
    needs_header = (not file_exists) or os.path.getsize(path) == 0
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=CSV_FIELDS)
        if needs_header:
            writer.writeheader()
        writer.writerow(row)


def main():
    args = parse_args()
    if args.duration <= 0:
        raise SystemExit("--duration must be positive")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency must be positive")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")

    keys = load_keys(args.keys_file)
    url = f"http://{args.host}:{args.port}/"
    deadline = time.perf_counter() + args.duration
    latencies = []
    errors = []
    threads = []

    start = time.perf_counter()
    for worker_id in range(args.concurrency):
        thread = threading.Thread(target=worker, args=(worker_id, args, keys, url, deadline, latencies, errors))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - start

    if errors:
        raise SystemExit(errors[0])

    latencies.sort()
    qps = len(latencies) / elapsed if elapsed > 0 else 0.0
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)

    print(f"system=clickhouse interface=http requests={len(latencies)} elapsed={elapsed:.3f}s")
    print(f"qps={qps:.2f} p50_ms={p50:.3f} p95_ms={p95:.3f} p99_ms={p99:.3f}")

    if args.output:
        append_csv(args.output, {
            "system": "clickhouse",
            "interface": "http",
            "dataset_size": args.dataset_size or "",
            "value_size": args.value_size or "",
            "batch_size": args.batch_size,
            "concurrency": args.concurrency,
            "qps": f"{qps:.2f}",
            "p50_ms": f"{p50:.3f}",
            "p95_ms": f"{p95:.3f}",
            "p99_ms": f"{p99:.3f}",
        })


if __name__ == "__main__":
    main()
