import uuid
import time
import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node",
    main_configs=["configs/overrides.xml"],
    stay_alive=True,
    env_variables={"MALLOC_CONF": "lg_prof_sample=20"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_memory_context_in_trace_log(started_cluster):
    if node.is_built_with_sanitizer():
        pytest.skip("sanitizers built without jemalloc")

    # Clean start (note, event_time >= now()-uptime(), can be flaky)
    node.query("TRUNCATE TABLE IF EXISTS system.trace_log")

    def get_trace_events(memory_context, memory_blocked_context, trace_type, query_id=None):
        res = int(node.query(f"""
        SELECT count() FROM system.trace_log
        WHERE
            memory_context = '{memory_context}'
            AND memory_blocked_context = '{memory_blocked_context}'
            AND trace_type = '{trace_type}'
            AND {'empty(query_id)' if query_id is None else f"query_id = '{query_id}'"}
        """).strip())
        logging.info('memory_context=%s/memory_blocked_context=%s/trace_type=%s/query_id=%s: %s',
                     memory_context, memory_blocked_context, trace_type, query_id, res)
        return res

    for _ in range(0, 15):
        # Generate some logs to generate entries with memory_blocked_context=Global and trace_type=JemallocSample
        for i in range(10):
            node.query("SELECT logTrace('foo')")
        query_id = uuid.uuid4().hex
        node.query("SELECT * FROM numbers(100000) ORDER BY number", query_id=query_id)

        node.query("SYSTEM FLUSH LOGS system.trace_log")
        if (
            get_trace_events("Unknown", "Max", "MemorySample", query_id) > 0 and
            get_trace_events("Unknown", "Max", "JemallocSample", query_id) > 0 and
            get_trace_events("Unknown", "Max", "JemallocSample") > 0 and
            get_trace_events("Unknown", "Global", "JemallocSample") > 0 and
            get_trace_events("Global", "Max", "Memory") > 0 and
            get_trace_events("Global", "Max", "MemoryPeak") > 0 and
            True
        ):
            break
        time.sleep(1)

    # For JemallocSample we have Global (for i.e. logging) and Max (for regular allocations) blocked memory tracker
    for memory_blocked_context in ["Global", "Max"]:
        assert get_trace_events("Unknown", memory_blocked_context, "JemallocSample") > 0

    for memory_context in ["Global"]:
        for trace_type in ["Memory", "MemoryPeak"]:
            assert get_trace_events(memory_context, "Max", trace_type) > 0

    assert get_trace_events("Unknown", "Max", "MemorySample", query_id) > 0
    # It is better not to check for memory_blocked_context=Global, since we
    # cannot be sure that allocations for logging for that tiny query will be
    # sampled by jemalloc.
    assert get_trace_events("Unknown", "Max", "JemallocSample", query_id) > 0

    # Graceful shutdown too slow due to flushing trace_log
    node.restart_clickhouse(kill=True)
