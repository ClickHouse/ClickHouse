import json
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager, nullcontext

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
coordinator = cluster.add_instance("coordinator", stay_alive=True, main_configs=["configs/profile_sampling.xml"])
middle = cluster.add_instance("middle", user_configs=["configs/delay.xml"], main_configs=["configs/profile_sampling.xml"])
leaf = cluster.add_instance("leaf", main_configs=["configs/profile_sampling.xml"])
nodes = (coordinator, middle, leaf)

SETTINGS = {
    "send_profile_traces": 1,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 65536,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_block_size": 1,
    "max_rows_to_read": 0,
    "max_execution_time": 30,
    "prefer_localhost_replica": 0,
    "use_hedged_requests": 0,
}
WORKLOAD = "SELECT hostName() AS host, sum(length(range(number + 100000))) AS total FROM numbers(128)"
EXPECTED_TOTAL = 128 * 100000 + 127 * 128 // 2
SAMPLE_TYPES = {"CPU", "Real", "Memory", "MemorySample", "MemoryPeak"}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def query_id():
    return "distributed_profile_" + uuid.uuid4().hex


def remote(address, query):
    return f"SELECT * FROM remote('{address}', view({query}))"


def nested(query=WORKLOAD):
    return remote("middle", remote("leaf", query))


def execute(transport, query, initial_id, settings=None):
    options = dict(SETTINGS, query_id=initial_id)
    options.update(settings or {})
    if transport == "Native":
        result = subprocess.run(
            coordinator.client.command + [f"--{key}={value}" for key, value in options.items()] + ["--print-profile-traces", "--query", query],
            capture_output=True,
            text=True,
            timeout=60,
        )
        samples = []
        error_lines = []
        for line in result.stderr.splitlines():
            if line.startswith('{"host_name":'):
                samples.append(json.loads(line))
            elif line:
                error_lines.append(line)
        error = "\n".join(error_lines)
        assert result.returncode == 0 or error, result.stderr
        return result.stdout, samples, error

    options.update(
        framing_output_format="JSONEachPacketString",
        http_wait_end_of_query=0,
        http_response_buffer_size=0,
        output_format_parallel_formatting=0,
    )
    response = requests.post(
        f"http://{coordinator.ip_address}:8123/",
        params=options,
        data=query,
        timeout=60,
    )
    packets = [json.loads(line) for line in response.text.splitlines() if line]
    assert packets, response.text
    assert packets[-1]["packet"] in ("progress", "exception"), packets[-1]
    error = packets[-1].get("exception", "")
    assert response.ok or error, (response.status_code, response.text)
    samples = [sample for packet in packets if packet["packet"] == "profile_traces" for sample in packet["profile_traces"]]
    data = "".join(packet["data"] for packet in packets if packet["packet"] == "data")
    return data, samples, error


def validate_samples(samples):
    for sample in samples:
        assert sample["host_name"] in {node.name for node in nodes}, sample
        assert sample["query_id"], sample
        assert len(sample["trace"]) == len(sample["symbols"]), sample
        trace_type = sample["trace_type"]
        if trace_type in SAMPLE_TYPES:
            assert sample["trace"], sample
        else:
            assert trace_type in {"Dropped", "Incomplete"}, sample
            assert not sample["trace"] and not sample["symbols"], sample
            assert int(sample["thread_id"]) == 0, sample
            assert int(sample["event_time_microseconds"]) == 0, sample
            if trace_type == "Dropped":
                assert int(sample["size"]) > 0, sample
            else:
                assert int(sample["size"]) == 0, sample


def sampled_hosts(samples):
    return {sample["host_name"] for sample in samples if sample["trace_type"] == "MemorySample" and int(sample["size"]) > 0 and any(sample["symbols"])}


def check_success(transport, query, expected_hosts, settings=None):
    observed_hosts = set()
    all_samples = []
    # The pipe is shared with other queries, so require positive sampling evidence
    # across a bounded number of executions while validating every received sample.
    for _ in range(4):
        initial_id = query_id()
        data, samples, error = execute(transport, query + " FORMAT TSV", initial_id, settings)
        assert not error, error
        assert sorted(data.splitlines()) == sorted(f"{host}\t{EXPECTED_TOTAL}" for host in expected_hosts), data
        validate_samples(samples)
        for sample in samples:
            if sample["host_name"] == coordinator.name:
                assert sample["query_id"] == initial_id, sample
            else:
                assert sample["query_id"] != initial_id, sample
        producer_ids = {host: {sample["query_id"] for sample in samples if sample["host_name"] == host} for host in expected_hosts}
        if len(expected_hosts) == 2:
            first, second = sorted(expected_hosts)
            assert producer_ids[first].isdisjoint(producer_ids[second]), producer_ids
        observed_hosts.update(sampled_hosts(samples))
        all_samples.extend(samples)
        if expected_hosts <= observed_hosts:
            return all_samples
    assert expected_hosts <= observed_hosts, (expected_hosts, observed_hosts)


@pytest.mark.parametrize("transport", ["Native", "HTTP"])
@pytest.mark.parametrize("topology", ["shards", "nested"])
def test_distributed_origins(transport, topology):
    if topology == "shards":
        check_success(transport, remote("middle,leaf", WORKLOAD), {"middle", "leaf"})
    else:
        check_success(transport, nested(), {"leaf"})


@contextmanager
def failpoint(node, name):
    node.query(f"SYSTEM ENABLE FAILPOINT {name}")
    try:
        yield
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {name}")


@pytest.mark.parametrize("transport", ["Native", "HTTP"])
@pytest.mark.parametrize("coordinator_overflow", [False, True])
def test_forwarded_losses(transport, coordinator_overflow):
    # Only the leaf samples allocations, so local losses cannot hide a missing forwarded delta.
    settings = {"memory_profiler_sample_probability": 0}
    with ExitStack() as users:
        queries = {}
        for probability in (0, 1):
            user = query_id()
            # Secondary settings are clamped before the request's memory tracker is configured.
            leaf.query(f"CREATE USER {user} IDENTIFIED WITH no_password SETTINGS memory_profiler_sample_probability = {probability} MIN {probability} MAX {probability}")
            users.callback(leaf.query, f"DROP USER {user}")
            leaf.query(f"GRANT SELECT ON *.* TO {user}")
            queries[probability] = remote("middle", f"SELECT * FROM remote('leaf', view({WORKLOAD}), '{user}')") + " FORMAT TSV"

        with failpoint(leaf, "profile_traces_queue_overflow"):
            with failpoint(coordinator, "profile_traces_queue_overflow") if coordinator_overflow else nullcontext():
                initial_id = query_id()
                data, samples, error = execute(transport, queries[1], initial_id, settings)
                assert not error, error
                assert data == f"leaf\t{EXPECTED_TOTAL}\n", data
                validate_samples(samples)
                dropped = [sample for sample in samples if sample["trace_type"] == "Dropped"]
                assert dropped, samples
                assert all(int(sample["size"]) > 0 for sample in dropped), dropped
                assert all(sample["host_name"] == coordinator.name and sample["query_id"] == initial_id for sample in dropped), dropped
                assert not any(sample["trace_type"] in SAMPLE_TYPES for sample in samples), sorted(
                    {(sample["host_name"], sample["trace_type"]) for sample in samples if sample["trace_type"] in SAMPLE_TYPES}
                )

                data, samples, error = execute(transport, queries[0], query_id(), settings)
                assert not error and data == f"leaf\t{EXPECTED_TOTAL}\n", (data, error)
                assert not samples, (
                    "sampling-disabled path generated local loss metadata",
                    sorted({(sample["host_name"], sample["trace_type"]) for sample in samples})[:8],
                )

                data, samples, error = execute(
                    transport,
                    queries[1],
                    query_id(),
                    dict(settings, send_profile_traces=0),
                )
                assert not error and data == f"leaf\t{EXPECTED_TOTAL}\n", (data, error)
                assert not samples, samples


def test_nested_flush_timeout():
    with failpoint(leaf, "profile_traces_flush_ack_timeout"):
        start = time.monotonic()
        # The nested table functions describe the leaf twice before executing it;
        # each of those three queries has its own ten-second flush deadline.
        data, samples, error = execute("HTTP", nested() + " FORMAT TSV", query_id(), {"max_execution_time": 45})
        assert not error and data == f"leaf\t{EXPECTED_TOTAL}\n", (data, error)
        # Five seconds of overhead still distinguishes three ten-second barriers from four.
        elapsed = time.monotonic() - start
        assert elapsed < 35, f"nested profiling flush exceeded three deadlines: {elapsed:.3f}s"
        validate_samples(samples)
        assert any(sample["trace_type"] == "Incomplete" for sample in samples), samples


def wait_for_no_queries(initial_id):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        active = [node.name for node in nodes if node.query(f"SELECT count() FROM system.processes WHERE initial_query_id = '{initial_id}'").strip() != "0"]
        if not active:
            return
        time.sleep(0.1)
    assert not active, active


def check_next_query(transport, previous_ids):
    samples = check_success(transport, nested(), {"leaf"})
    assert not previous_ids.intersection(sample["query_id"] for sample in samples), samples
    data, samples, error = execute(transport, nested() + " FORMAT TSV", query_id(), {"send_profile_traces": 0})
    assert not error and data == f"leaf\t{EXPECTED_TOTAL}\n", (data, error)
    assert not samples, samples


@pytest.mark.parametrize("transport", ["Native", "HTTP"])
def test_nested_exception(transport):
    initial_id = query_id()
    _, samples, error = execute(
        transport,
        nested("SELECT length(range(number + 100000)), throwIf(number = 8, 'distributed profile exception') FROM numbers(16)") + " FORMAT Null",
        initial_id,
    )
    assert "distributed profile exception" in error, error
    validate_samples(samples)
    wait_for_no_queries(initial_id)
    check_next_query(transport, {initial_id} | {sample["query_id"] for sample in samples})


@pytest.mark.parametrize("transport", ["Native", "HTTP"])
def test_nested_cancellation(transport):
    initial_id = query_id()
    previous_ids = {initial_id}
    with ThreadPoolExecutor(max_workers=1) as executor, ExitStack() as failpoints:
        request = executor.submit(
            execute,
            transport,
            nested("SELECT sum(length(range(number % 128 + 100000))) FROM numbers(1000000000000)") + " FORMAT TSV",
            initial_id,
        )
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                running = leaf.query(f"SELECT query_id FROM system.processes WHERE initial_query_id = '{initial_id}' AND read_rows > 1").splitlines()
                if running:
                    previous_ids.update(running)
                    break
                assert not request.done(), request.result() if request.done() else ""
                time.sleep(0.1)
            assert running, "nested leaf query did not process rows"
            # Schema discovery has finished. The peers receive Native Cancel, while the
            # coordinator's KILL QUERY follows ordinary error finalization and must still drain.
            for node in (middle, leaf):
                failpoints.enter_context(failpoint(node, "profile_traces_flush_ack_timeout"))
        finally:
            cancel_started = time.monotonic()
            coordinator.query(f"KILL QUERY WHERE query_id = '{initial_id}' SYNC", timeout=30)
        _, samples, error = request.result(timeout=30)
        wait_for_no_queries(initial_id)
        cancel_elapsed = time.monotonic() - cancel_started
        assert cancel_elapsed < 5, f"nested cancellation waited for a collector deadline: {cancel_elapsed:.3f}s"
        assert not any(sample["trace_type"] == "Incomplete" for sample in samples), samples
    assert "QUERY_WAS_CANCELLED" in error or "Query was cancelled" in error, error
    validate_samples(samples)
    previous_ids.update(sample["query_id"] for sample in samples)
    check_next_query(transport, previous_ids)


def test_hedged_replica_change():
    if coordinator.is_built_with_thread_sanitizer():
        pytest.skip("Hedged requests do not support TSan")
    delay_file = "/etc/clickhouse-server/users.d/delay.xml"
    try:
        middle.replace_config(
            delay_file,
            "<clickhouse><profiles><default><sleep_after_receiving_query_ms>5000</sleep_after_receiving_query_ms></default></profiles></clickhouse>",
        )
        middle.http_query("SYSTEM RELOAD USERS", method="POST")
        assert middle.http_query("SELECT getSetting('sleep_after_receiving_query_ms')").strip() == "5000"
        # `TCPHandler` caches the delay per connection. `SYSTEM DROP CONNECTIONS CACHE`
        # clears only HTTP pools, so restart to renew the coordinator's native connections.
        coordinator.restart_clickhouse()
        before = int(coordinator.query("SELECT sum(value) FROM system.events WHERE event = 'HedgedRequestsChangeReplica'"))
        check_success(
            "HTTP",
            remote("middle|leaf", WORKLOAD),
            {"leaf"},
            {
                "load_balancing": "in_order",
                "use_hedged_requests": 1,
                "hedged_connection_timeout_ms": 100,
                "receive_data_timeout_ms": 100,
                "allow_changing_replica_until_first_data_packet": 1,
                "async_socket_for_remote": 1,
            },
        )
        after = int(coordinator.query("SELECT sum(value) FROM system.events WHERE event = 'HedgedRequestsChangeReplica'"))
        assert after > before, (before, after)
    finally:
        middle.replace_config(
            delay_file,
            "<clickhouse><profiles><default><sleep_after_receiving_query_ms>0</sleep_after_receiving_query_ms></default></profiles></clickhouse>",
        )
        middle.http_query("SYSTEM RELOAD USERS", method="POST")
        coordinator.restart_clickhouse()
