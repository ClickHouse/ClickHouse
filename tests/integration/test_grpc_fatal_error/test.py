import os
import sys
import threading
import time
import uuid

import grpc
import pytest

from helpers.cluster import SANITIZER_SIGN, ClickHouseCluster

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


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        # The stack trace printed for SIGTSTP is logged with the <Fatal> level.
        cluster.shutdown(ignore_fatal=True)


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

    client_thread = threading.Thread(target=run_query)
    client_thread.start()

    # A thread of the query's pipeline, not the call thread.
    tid = ""
    for _ in range(300):
        tid = node.query(
            f"SELECT thread_id FROM system.stack_trace WHERE query_id = '{query_id}' "
            "AND thread_name != 'gRPCServerCall' LIMIT 1"
        ).strip()
        if tid or not client_thread.is_alive():
            break
        time.sleep(0.1)
    assert tid, f"no pipeline thread found, client errors: {errors}"

    # The pipeline can run only a few rows ahead of the call thread, so once 20 rows are read the call thread has
    # pulled some of them and moved the query's first log lines out of the logs queue.
    read_rows = 0
    for _ in range(300):
        read_rows = int(
            node.query(
                f"SELECT read_rows FROM system.processes WHERE query_id = '{query_id}'"
            ).strip()
            or 0
        )
        if read_rows >= 20 or not client_thread.is_alive():
            break
        time.sleep(0.1)
    assert read_rows >= 20, f"the query makes no progress, client errors: {errors}"
    node.exec_in_container(["python3", "-c", TGKILL, tid], user="root")

    client_thread.join(timeout=120)
    channel.close()
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
