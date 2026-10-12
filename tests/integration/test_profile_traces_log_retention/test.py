import json
import subprocess
import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
coordinator = cluster.add_instance("coordinator", main_configs=["configs/trace_log.xml"])
leaf = cluster.add_instance("leaf", main_configs=["configs/trace_log.xml"])

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
    "max_block_size": 65536,
    "prefer_localhost_replica": 0,
    "use_hedged_requests": 0,
}
SAMPLE_TYPES = {"CPU", "Real", "Memory", "MemorySample", "MemoryPeak"}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def execute(transport, query, query_id, settings=None):
    options = dict(SETTINGS, query_id=query_id)
    options.update(settings or {})
    if transport == "Native":
        result = subprocess.run(
            coordinator.client.command + [f"--{key}={value}" for key, value in options.items()] + ["--print-profile-traces", "--query", query],
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stderr
        assert not result.stdout, result.stdout
        samples = [json.loads(line) for line in result.stderr.splitlines() if line]
    else:
        options.update(
            framing_output_format="JSONEachPacketString",
            http_wait_end_of_query=0,
            http_response_buffer_size=0,
            output_format_parallel_formatting=0,
        )
        response = requests.post(f"http://{coordinator.ip_address}:8123/", params=options, data=query, timeout=90)
        assert response.ok, response.text
        packets = [json.loads(line) for line in response.text.splitlines() if line]
        assert packets and packets[-1]["packet"] == "progress", packets[-1:]
        assert not any(packet["packet"] == "data" for packet in packets), packets
        samples = [sample for packet in packets if packet["packet"] == "profile_traces" for sample in packet["profile_traces"]]
    assert {sample["trace_type"] for sample in samples} <= SAMPLE_TYPES | {"Dropped", "Incomplete"}, samples
    return [sample for sample in samples if sample["trace_type"] in SAMPLE_TYPES]


@pytest.mark.parametrize("transport", ["Native", "HTTP"])
@pytest.mark.parametrize("remote", [False, True], ids=["local", "remote"])
def test_profile_events_retained(transport, remote):
    query_id = "profile_event_retention_" + uuid.uuid4().hex
    query = "SELECT length(range(number + 100000)) FROM numbers(8)"
    if remote:
        query = f"SELECT * FROM remote('leaf', view({query}))"
    samples = execute(
        transport,
        query + " FORMAT Null",
        query_id,
        {"memory_profiler_step": 65536, "trace_profile_events": 1, "trace_profile_events_list": "FunctionExecute"},
    )
    if remote:
        samples = [sample for sample in samples if sample["query_id"] != query_id]
    else:
        assert all(sample["query_id"] == query_id for sample in samples), samples
    producer = leaf if remote else coordinator
    assert samples and all(sample["host_name"] == producer.name for sample in samples), samples
    assert any(sample["trace_type"] == "MemorySample" for sample in samples), samples
    observed_ids = {sample["query_id"] for sample in samples}
    assert all(observed_ids), samples
    ids = ", ".join(f"'{value}'" for value in sorted(observed_ids))

    # Only this fixture produces traces here, so flushing cannot wait on other tests' profiling backlog.
    producer.query("SYSTEM FLUSH LOGS trace_log", timeout=20)
    assert (
        producer.query(
            f"""
            SELECT countIf(query_id IN ({ids})) > 0
            FROM system.trace_log
            WHERE event_date >= today() - 1 AND event_time >= now() - 600
              AND trace_type = 'ProfileEvent' AND event = 'FunctionExecute' AND notEmpty(trace)
            """,
            timeout=20,
        )
        == "1\n"
    )


@pytest.mark.parametrize("transport", ["Native", "HTTP"])
def test_jemalloc_samples_retained(transport):
    if coordinator.query("SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC'").strip() != "1":
        pytest.skip("Requires the jemalloc allocation profiler")
    query_id = "jemalloc_retention_" + uuid.uuid4().hex
    samples = execute(
        transport,
        "SELECT number FROM numbers(1000000) ORDER BY number FORMAT Null",
        query_id,
        {"jemalloc_enable_profiler": 1, "jemalloc_collect_profile_samples_in_trace_log": 1},
    )
    assert samples and all(sample["query_id"] == query_id for sample in samples), samples
    assert any(sample["trace_type"] == "MemorySample" for sample in samples), samples
    coordinator.query("SYSTEM FLUSH LOGS trace_log", timeout=20)
    assert (
        coordinator.query(
            f"""
            SELECT countIf(trace_type = 'JemallocSample' AND size > 0 AND ptr != 0 AND notEmpty(trace)) > 0
            FROM system.trace_log
            WHERE event_date >= today() - 1 AND event_time >= now() - 600 AND query_id = '{query_id}'
            """,
            timeout=20,
        )
        == "1\n"
    )
