#!/usr/bin/env python3

import argparse
import csv
import os
import socket
import threading
import time


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


class RedisErrorResponse(Exception):
    pass


class NullBulkString:
    pass


NULL_BULK_STRING = NullBulkString()


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Redis GET/MGET with raw sockets.")
    parser.add_argument("--host", default="127.0.0.1", help="Redis host.")
    parser.add_argument("--port", type=int, default=6379, help="Redis port.")
    parser.add_argument("--db", type=int, default=0, help="Redis database number to select.")
    parser.add_argument("--keys-file", required=True, help="File with one key per line.")
    parser.add_argument("--duration", type=float, default=30, help="Benchmark duration in seconds.")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of worker threads.")
    parser.add_argument("--batch-size", type=int, default=1, help="Keys per request.")
    parser.add_argument("--output", help="Optional CSV output file. One row is appended.")
    parser.add_argument("--dataset-size", help="Dataset size label for CSV output.")
    parser.add_argument("--value-size", help="Value size label for CSV output.")
    return parser.parse_args()


def encode_command(*parts):
    encoded = [f"*{len(parts)}\r\n".encode("ascii")]
    for part in parts:
        if isinstance(part, bytes):
            value = part
        else:
            value = str(part).encode("utf-8")
        encoded.append(f"${len(value)}\r\n".encode("ascii"))
        encoded.append(value)
        encoded.append(b"\r\n")
    return b"".join(encoded)


def read_line(sock):
    line = sock.readline()
    if not line:
        raise EOFError("connection closed while reading RESP line")
    if not line.endswith(b"\r\n"):
        raise RuntimeError(f"invalid RESP line: {line!r}")
    return line[:-2]


def read_exact(sock, size):
    data = sock.read(size)
    if data is None or len(data) != size:
        raise EOFError("connection closed while reading RESP payload")
    return data


def read_response(sock):
    prefix = read_exact(sock, 1)
    if prefix == b"+":
        return read_line(sock).decode("utf-8", errors="replace")
    if prefix == b"-":
        return RedisErrorResponse(read_line(sock).decode("utf-8", errors="replace"))
    if prefix == b"$":
        size = int(read_line(sock))
        if size == -1:
            return NULL_BULK_STRING
        data = read_exact(sock, size)
        trailer = read_exact(sock, 2)
        if trailer != b"\r\n":
            raise RuntimeError("invalid RESP bulk string trailer")
        return data
    if prefix == b"*":
        size = int(read_line(sock))
        if size == -1:
            return None
        return [read_response(sock) for _ in range(size)]
    raise RuntimeError(f"unsupported RESP response prefix: {prefix!r}")


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


def validate_get_response(response):
    if isinstance(response, RedisErrorResponse):
        raise response
    if response is NULL_BULK_STRING or isinstance(response, bytes):
        return
    raise RuntimeError(f"GET returned unexpected RESP value: {type(response).__name__}")


def validate_mget_response(response):
    if isinstance(response, RedisErrorResponse):
        raise response
    if isinstance(response, list):
        for item in response:
            if isinstance(item, RedisErrorResponse):
                raise item
            if item is not NULL_BULK_STRING and not isinstance(item, bytes):
                raise RuntimeError(f"MGET returned unexpected array item: {type(item).__name__}")
        return
    raise RuntimeError(f"MGET returned unexpected RESP value: {type(response).__name__}")


def worker(worker_id, args, keys, deadline, stop_event, latencies, errors):
    local_latencies = []
    key_count = len(keys)
    index = worker_id * args.batch_size

    try:
        with socket.create_connection((args.host, args.port), timeout=10) as client:
            with client.makefile("rb") as client_file:
                client.sendall(encode_command("SELECT", args.db))
                response = read_response(client_file)
                if isinstance(response, RedisErrorResponse):
                    errors.append(f"SELECT returned Redis error: {response}")
                    stop_event.set()
                    return
                if response != "OK":
                    errors.append(f"SELECT returned unexpected response: {response!r}")
                    stop_event.set()
                    return

                while time.perf_counter() < deadline and not stop_event.is_set():
                    batch = [keys[(index + offset) % key_count] for offset in range(args.batch_size)]
                    index = (index + args.batch_size * args.concurrency) % key_count

                    if args.batch_size == 1:
                        command_name = "GET"
                        command = encode_command("GET", batch[0])
                    else:
                        command_name = "MGET"
                        command = encode_command("MGET", *batch)

                    start = time.perf_counter()
                    try:
                        client.sendall(command)
                        response = read_response(client_file)
                        if args.batch_size == 1:
                            validate_get_response(response)
                        else:
                            validate_mget_response(response)
                    except RedisErrorResponse as exc:
                        errors.append(f"{command_name} returned Redis error: {exc}")
                        stop_event.set()
                        break
                    local_latencies.append((time.perf_counter() - start) * 1000.0)
    except Exception as exc:
        errors.append(str(exc))
        stop_event.set()
    finally:
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
    deadline = time.perf_counter() + args.duration
    stop_event = threading.Event()
    latencies = []
    errors = []
    threads = []

    start = time.perf_counter()
    for worker_id in range(args.concurrency):
        thread = threading.Thread(target=worker, args=(worker_id, args, keys, deadline, stop_event, latencies, errors))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - start

    latencies.sort()
    qps = len(latencies) / elapsed if elapsed > 0 else 0.0
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)

    print(f"system=redis interface=resp_raw requests={len(latencies)} elapsed={elapsed:.3f}s")
    print(f"qps={qps:.2f} p50_ms={p50:.3f} p95_ms={p95:.3f} p99_ms={p99:.3f} errors={len(errors)}")

    if errors:
        raise SystemExit(errors[0])

    if args.output:
        append_csv(args.output, {
            "system": "redis",
            "interface": "resp_raw",
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
