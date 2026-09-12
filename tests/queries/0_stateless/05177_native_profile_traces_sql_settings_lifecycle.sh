#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_DATABASE" "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" <<'PY'
import json
import select
import shlex
import socket
import subprocess
import sys
import threading
import uuid

client_command, database, server_host, server_port = sys.argv[1:]
client = shlex.split(client_command)
database = database.replace("`", "``")
source = f"`{database}`.profile_native_sql_source"
backup_prefix = "profile_native_sql_" + uuid.uuid4().hex
settings = {
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 65536,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_insert_threads": 1,
    "max_block_size": 65536,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "format": "TabSeparated",
}


def native_arguments(options):
    arguments = []
    index = 0
    while index < len(client):
        argument = client[index]
        name = argument.split("=", 1)[0].removeprefix("--")
        if argument.startswith("--") and name in options:
            if "=" not in argument and index + 1 < len(client) and not client[index + 1].startswith("-"):
                index += 1
        else:
            arguments.append(argument)
        index += 1
    return arguments + [f"--{key}={value}" for key, value in options.items()]


def run(command):
    try:
        return subprocess.run(command, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        raise AssertionError("native BACKUP exceeded its process deadline") from None


def control(query):
    options = dict(settings, send_profile_traces=0, memory_profiler_sample_probability=0)
    result = run(native_arguments(options) + ["--query", query])
    assert result.returncode == 0, result.stderr


def sql_opt_in(options, query):
    # The normal client promotes inline settings into its wire settings. Replacing only
    # the same-width SQL literal preserves wire opt-out and exercises server-side opt-in.
    marker = b"SETTINGS send_profile_traces = 0"
    assert query.encode().count(marker) == 1, query
    replacements = []
    errors = []
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(15)

    def relay():
        try:
            downstream, _ = listener.accept()
            with downstream, socket.create_connection((server_host, int(server_port)), timeout=15) as upstream:
                pending = b""
                readers = [downstream, upstream]
                while readers:
                    ready, _, _ = select.select(readers, [], [], 15)
                    if not ready:
                        raise TimeoutError("native SQL opt-in relay made no progress")
                    for connection in ready:
                        data = connection.recv(65536)
                        target = upstream if connection is downstream else downstream
                        if not data:
                            if connection is downstream and pending:
                                upstream.sendall(pending)
                            target.shutdown(socket.SHUT_WR)
                            readers.remove(connection)
                            continue
                        if connection is downstream:
                            data = pending + data
                            replacements.extend([1] * data.count(marker))
                            data = data.replace(marker, marker[:-1] + b"1")
                            # Retain only a possible marker prefix so the handshake is
                            # forwarded immediately, even if the SQL spans socket reads.
                            keep = min(len(marker) - 1, len(data))
                            while keep and not data.endswith(marker[:keep]):
                                keep -= 1
                            pending = data[-keep:] if keep else b""
                            data = data[:-keep] if keep else data
                        if data:
                            target.sendall(data)
        except OSError as error:
            errors.append(str(error))
        finally:
            listener.close()

    proxied = dict(options, host="127.0.0.1", port=listener.getsockname()[1])
    worker = threading.Thread(target=relay, daemon=True)
    worker.start()
    try:
        result = run(native_arguments(proxied) + ["--print-profile-traces", "--query", query])
    finally:
        worker.join(timeout=16)
    assert not worker.is_alive(), "native SQL opt-in relay did not finish"
    assert not errors, errors
    assert len(replacements) == 1, f"expected exactly one SQL replacement, got {len(replacements)}"
    return result


def backup(mode):
    query_id = "profile_native_sql_" + uuid.uuid4().hex
    options = dict(settings, query_id=query_id, send_profile_traces=int(mode == "wire"))
    query = f"BACKUP TABLE {source} TO Disk('backups', '{backup_prefix}_{mode}')"
    if mode != "wire":
        query += " SETTINGS send_profile_traces = 0"
    if mode == "sql":
        result = sql_opt_in(options, query)
    else:
        result = run(native_arguments(options) + ["--print-profile-traces", "--query", query])
    assert result.returncode == 0, (mode, result.stdout, result.stderr)
    rows = [line.split("\t") for line in result.stdout.splitlines()]
    assert len(rows) == 1 and len(rows[0]) == 2 and rows[0][1] == "BACKUP_CREATED", (mode, result.stdout)
    samples = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{"host_name":')]
    assert all(sample["query_id"] == query_id for sample in samples), "trace subscription crossed queries"
    assert not any(sample["trace_type"] in {"Dropped", "Incomplete"} for sample in samples), "small query lost samples"
    if mode == "disabled":
        assert not samples, "wire and SQL opt-out emitted traces"
        return
    # `BackupStarter` runs on a thread inherited from the query's group before
    # `executeQuery` returns. Its allocations exclude late transport-only samples.
    assert any(
        sample["trace_type"] == "MemorySample"
        and int(sample["size"]) > 0
        and any("DB::BackupsWorker::BackupStarter::doBackup" in frame for frame in sample["symbols"])
        and not any("DB::TCPHandler::" in frame for frame in sample["symbols"])
        for sample in samples
    ), f"{mode}: no background BACKUP allocation among {len(samples)} trace rows"


try:
    control(f"CREATE TABLE {source} (x UInt64) ENGINE=Memory")
    control(f"INSERT INTO {source} SELECT number FROM numbers(100000)")
    backup("wire")
    print("Native wire opt-in captures background BACKUP allocations")
    backup("disabled")
    print("Native wire and SQL opt-out complete BACKUP without traces")
    backup("sql")
    print("Native SQL-only opt-in captures background BACKUP allocations after one SQL rewrite")
finally:
    control(f"DROP TABLE IF EXISTS {source} SYNC")
PY
