"""The maximum entry size of the query cache (server setting `query_cache.max_entry_size_in_bytes`) also applies to the query cache on
disk, in particular when writes to the in-memory query cache are disabled and the on-disk cache is the only backend.

This needs an integration test because the limit is a server setting, and `clickhouse-local` (used by the stateless tests of the query
cache on disk) hardcodes it to 0.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["config.d/query_cache_on_disk.xml"],
    stay_alive=True,
)

SETTINGS = (
    "use_query_cache = 1, query_cache_on_disk_cache_name = 'cache_for_query_results', "
    "enable_writes_to_query_cache = 0"
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_event(name):
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{name}'"
        ).strip()
        or 0
    )


def test_oversized_result_is_not_stored_on_disk(started_cluster):
    # The limit is 1 byte here, so even this tiny result (a `ColumnConst`, which is stored in its compact representation) exceeds it.
    query = f"SELECT 1 FROM numbers(1000) SETTINGS {SETTINGS}"

    written_before = get_event("QueryCacheOnDiskWrittenBytes")
    node.query(query)
    assert get_event("QueryCacheOnDiskWrittenBytes") == written_before

    # Nothing was stored, so a repeated run cannot be served from disk.
    hits_before = get_event("QueryCacheOnDiskHits")
    node.query(query)
    assert get_event("QueryCacheOnDiskHits") == hits_before


def test_result_within_the_limit_is_stored_on_disk(started_cluster):
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/query_cache_on_disk.xml",
        "<max_entry_size_in_bytes>1</max_entry_size_in_bytes>",
        "<max_entry_size_in_bytes>1073741824</max_entry_size_in_bytes>",
    )
    node.restart_clickhouse()

    query = f"SELECT number FROM numbers(1000) SETTINGS {SETTINGS}"

    written_before = get_event("QueryCacheOnDiskWrittenBytes")
    node.query(query)
    assert get_event("QueryCacheOnDiskWrittenBytes") > written_before

    hits_before = get_event("QueryCacheOnDiskHits")
    node.query(query)
    assert get_event("QueryCacheOnDiskHits") == hits_before + 1
