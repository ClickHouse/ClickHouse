# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

# This test verifies that memory tracking does not have significant drift,
# in other words, every allocation should be taken into account at the global
# memory tracker.
#
# So we are running some queries with GROUP BY to make some allocations,
# and after we are checking MemoryTracking metric from system.metrics,
# and check that it does not changes too frequently.
#
# Also note, that syncing MemoryTracking with RSS had been disabled in
# asynchronous_metrics_update_period_s.xml.

import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/no_system_log.xml",
        "configs/asynchronous_metrics_update_period_s.xml",
    ],
    user_configs=[
        "configs/users.d/overrides.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


query_settings = {
    "max_threads": 1,
    "log_queries": 0,
}
sample_query = "SELECT groupArray(repeat('a', 1000)) FROM numbers(10000) GROUP BY number%10 FORMAT JSON"


def query(*args, **kwargs):
    if "settings" not in kwargs:
        kwargs["settings"] = query_settings
    else:
        kwargs["settings"].update(query_settings)
    return node.query(*args, **kwargs)


def http_query(*args, **kwargs):
    if "params" not in kwargs:
        kwargs["params"] = query_settings
    else:
        kwargs["params"].update(query_settings)
    return node.http_query(*args, **kwargs)


def get_MemoryTracking():
    return int(
        http_query("SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'")
    )


def check_memory(memory):
    # bytes -> megabytes
    memory = [*map(lambda x: int(int(x) / 1024 / 1024), memory)]
    # 3 changes to MemoryTracking is minimum, since:
    # - this is not that high to not detect inacuracy
    # - memory can go like X/X+N due to some background allocations
    # - memory can go like X/X+N/X, so at least 2 changes
    changes_allowed = 3
    # if number of samples is large enough, use 10% from them
    # (actually most of the time there will be only few changes, it was made 10% to avoid flackiness)
    changes_allowed_auto = int(len(memory) * 0.1)
    changes_allowed = max(changes_allowed_auto, changes_allowed)

    changed = len(set(memory))
    logging.info(
        "Changes: allowed=%s, actual=%s, sample=%s",
        changes_allowed,
        changed,
        len(memory),
    )
    assert changed < changes_allowed


def test_http():
    memory = []
    memory.append(get_MemoryTracking())
    for _ in range(100):
        http_query(sample_query)
        memory.append(get_MemoryTracking())
    check_memory(memory)


def test_tcp_multiple_sessions():
    memory = []
    memory.append(get_MemoryTracking())
    for _ in range(100):
        query(sample_query)
        memory.append(get_MemoryTracking())
    check_memory(memory)


def test_tcp_single_session():
    memory = []
    memory.append(get_MemoryTracking())
    sample_queries = [
        sample_query,
        "SELECT metric, value FROM system.metrics WHERE metric = 'MemoryTracking'",
    ] * 100
    rows = query(";".join(sample_queries))
    memory = rows.split("\n")
    memory = filter(lambda x: x.startswith("MemoryTracking"), memory)
    memory = map(lambda x: x.split("\t")[1], memory)
    memory = [*memory]
    check_memory(memory)
