import os
import subprocess
import sys
import threading
import time
import uuid

import grpc
import pytest

from helpers.cluster import SANITIZER_SIGN, ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, wait_condition

script_dir = os.path.dirname(os.path.realpath(__file__))
pb2_dir = os.path.join(script_dir, "pb2")
if pb2_dir not in sys.path:
    sys.path.append(pb2_dir)
import clickhouse_grpc_pb2  # Execute pb2/generate.py to generate these modules.
import clickhouse_grpc_pb2_grpc

GRPC_PORT = 9100

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    # Bug in TSAN reproduces in gRPC tests https://github.com/grpc/grpc/issues/29550#issuecomment-1188085387
    env_variables={
        "TSAN_OPTIONS": "report_atomic_races=0 " + os.getenv("TSAN_OPTIONS", default="")
    },
    stay_alive=True,
)

# SIGTSTP makes the server print the stack trace of the signaled thread and run the fatal error callbacks of its
# query, without terminating the process.
TGKILL = """
import ctypes, platform, signal, sys
tid = int(sys.argv[1])
with open(f"/proc/{tid}/status") as f:
    tgid = int([line for line in f if line.startswith("Tgid:")][0].split()[1])
syscall_tgkill = {"x86_64": 234, "aarch64": 131}[platform.machine()]
if ctypes.CDLL(None, use_errno=True).syscall(syscall_tgkill, tgid, tid, signal.SIGTSTP) != 0:
    sys.exit(f"tgkill failed: errno {ctypes.get_errno()}")
"""

# Takes the first result and no more. Its gRPC library keeps reading the socket, so the call stalls on HTTP/2 flow
# control, not on TCP, where gRPC aborts the connection after `TCP_USER_TIMEOUT` (20 s). `results` stays referenced:
# gRPC cancels the call of a collected response iterator.
STALLED_CLIENT = """
import sys, time
import grpc
sys.path.append(sys.argv[1])
import clickhouse_grpc_pb2, clickhouse_grpc_pb2_grpc
channel = grpc.insecure_channel(sys.argv[2])
grpc.channel_ready_future(channel).result(timeout=30)
stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
query_info = clickhouse_grpc_pb2.QueryInfo(
    query="SELECT number, repeat('x', 1000) FROM numbers(1000000000) SETTINGS max_block_size = 1000",
    query_id=sys.argv[3],
    output_format="TabSeparated",
)
results = stub.ExecuteQueryWithStreamOutput(query_info)
next(results)
time.sleep(3600)
"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        # The stack trace printed for SIGTSTP is logged with the <Fatal> level.
        cluster.shutdown(ignore_fatal=True)


def start_call(call, query_info):
    results = []
    errors = []

    channel = grpc.insecure_channel(f"{cluster.get_instance_ip('node')}:{GRPC_PORT}")
    grpc.channel_ready_future(channel).result(timeout=30)
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)

    def run_query():
        try:
            if call == "ExecuteQuery":
                results.append(stub.ExecuteQuery(query_info))
            else:
                results.extend(stub.ExecuteQueryWithStreamOutput(query_info))
        except Exception as e:
            errors.append(e)
        finally:
            channel.close()

    client_thread = threading.Thread(target=run_query)
    client_thread.start()
    return client_thread, results, errors


def wait_for_progress(
    query_id, thread_condition, progress_column, client_thread, errors
):
    """Returns a thread of the query once the query has processed 20 rows."""
    tid = ""
    for _ in range(300):
        tid = node.query(
            f"SELECT thread_id FROM system.stack_trace WHERE query_id = '{query_id}' "
            f"AND {thread_condition} LIMIT 1"
        ).strip()
        if tid or not client_thread.is_alive():
            break
        time.sleep(0.1)
    assert tid, f"no thread of the query found, client errors: {errors}"

    rows = 0
    for _ in range(300):
        rows = int(
            node.query(
                f"SELECT {progress_column} FROM system.processes WHERE query_id = '{query_id}'"
            ).strip()
            or 0
        )
        if rows >= 20 or not client_thread.is_alive():
            break
        time.sleep(0.1)
    assert rows >= 20, f"the query makes no progress, client errors: {errors}"
    return tid


def check_fatal_error(query_id, client_thread, results, errors):
    client_thread.join(timeout=120)
    assert not client_thread.is_alive()
    assert not errors

    assert results and results[-1].exception.name == "FatalError"
    logs = [log.text for result in results for log in result.logs]
    assert any("Received signal" in text for text in logs)
    assert any(
        log.source == "executeQuery" for result in results for log in result.logs
    )

    assert node.query("SELECT 1") == "1\n"
    deadline = time.monotonic() + 30
    while (
        node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        )
        != "0\n"
    ):
        assert time.monotonic() < deadline, "the query is still running"
        time.sleep(0.1)

    assert not node.contains_in_log("Cannot write to finalized buffer", from_host=True)
    assert not node.contains_in_log(
        "WriteBuffer is neither finalized nor canceled", from_host=True
    )
    assert not node.contains_in_log(
        SANITIZER_SIGN, from_host=True, filename="stderr.log"
    )


@pytest.mark.parametrize(
    "call, output_compression_type",
    [
        ("ExecuteQueryWithStreamOutput", ""),
        ("ExecuteQuery", ""),
        ("ExecuteQueryWithStreamOutput", "lz4"),
    ],
)
def test_fatal_error_in_pipeline_thread(call, output_compression_type):
    query_id = f"grpc_fatal_error_{uuid.uuid4().hex}"
    query_info = clickhouse_grpc_pb2.QueryInfo(
        query="SELECT number, sleepEachRow(0.1) FROM numbers(1200) SETTINGS max_block_size = 1, max_threads = 1",
        query_id=query_id,
        settings={"send_logs_level": "debug"},
        output_format="TabSeparated",
        output_compression_type=output_compression_type,
    )
    client_thread, results, errors = start_call(call, query_info)

    # A thread of the query's pipeline, not the call thread. The pipeline can run only a few rows ahead of the call
    # thread, so once 20 rows are read the call thread has pulled some of them and moved the query's first log lines
    # out of the logs queue.
    tid = wait_for_progress(
        query_id, "thread_name != 'gRPCServerCall'", "read_rows", client_thread, errors
    )
    node.exec_in_container(["python3", "-c", TGKILL, tid], user="root")

    check_fatal_error(query_id, client_thread, results, errors)


def test_fatal_error_during_insert():
    node.query("DROP TABLE IF EXISTS fatal_insert")
    node.query(
        "CREATE TABLE fatal_insert (x UInt64, y UInt8 DEFAULT sleepEachRow(0.1)) ENGINE = Null"
    )

    query_id = f"grpc_fatal_error_insert_{uuid.uuid4().hex}"
    query_info = clickhouse_grpc_pb2.QueryInfo(
        query="INSERT INTO fatal_insert (x) FORMAT TabSeparated",
        query_id=query_id,
        input_data="".join(f"{i}\n" for i in range(1200)).encode(),
        settings={
            "send_logs_level": "debug",
            "max_insert_block_size": "1",
            "min_insert_block_size_rows": "1",
            "input_format_parallel_parsing": "0",
        },
    )
    client_thread, results, errors = start_call("ExecuteQuery", query_info)

    # The call thread is the only thread of the query: it parses and inserts the rows, 0.1 s each for the default of `y`.
    tid = wait_for_progress(query_id, "1", "written_rows", client_thread, errors)
    node.exec_in_container(["python3", "-c", TGKILL, tid], user="root")

    check_fatal_error(query_id, client_thread, results, errors)

    node.query("DROP TABLE fatal_insert")


def test_fatal_error_when_client_does_not_read():
    query_id = f"grpc_fatal_error_stalled_client_{uuid.uuid4().hex}"
    client = subprocess.Popen(
        [
            sys.executable,
            "-c",
            STALLED_CLIENT,
            pb2_dir,
            f"{cluster.get_instance_ip('node')}:{GRPC_PORT}",
            query_id,
        ]
    )
    try:
        # The query stops reading rows once the call thread waits for a write that the client does not read.
        rows = ""
        for _ in range(120):
            previous_rows = rows
            rows = node.query(
                f"SELECT read_rows FROM system.processes WHERE query_id = '{query_id}'"
            ).strip()
            if rows and rows == previous_rows:
                break
            time.sleep(1)
        assert rows and rows == previous_rows, "the query does not stall"

        tid = node.query(
            f"SELECT thread_id FROM system.stack_trace WHERE query_id = '{query_id}' "
            "AND thread_name = 'gRPCServerCall'"
        ).strip()
        assert tid
        assert client.poll() is None, "the client exited"
        node.exec_in_container(["python3", "-c", TGKILL, tid], user="root")

        # The signal listener has taken SIGTSTP, so it can get SIGHUP only after the fatal error callback returns.
        node.wait_for_log_line(rf"\(query_id: {query_id}\)", timeout=120)

        # SIGHUP reopens the log, and `rotateOnOpen` replaces the file.
        def log_file_inode():
            return node.exec_in_container(
                [
                    "stat",
                    "-c",
                    "%i",
                    "/var/log/clickhouse-server/clickhouse-server.log",
                ],
                nothrow=True,
            )

        inode = log_file_inode()
        node.rotate_logs()
        wait_condition(
            log_file_inode,
            lambda new_inode: new_inode != inode,
            max_attempts=240,
            delay=0.5,
        )

        assert_eq_with_retry(
            node,
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'",
            "0",
            retry_count=240,
            sleep_time=0.5,
        )
        assert node.query("SELECT 1") == "1\n"
        assert not node.contains_in_log(
            SANITIZER_SIGN, from_host=True, filename="stderr.log"
        )
    finally:
        client.kill()
        client.wait()
