# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_configuration.xml"],
    tmpfs=["/local_disk:size=50M", "/tiny_local_cache:size=12M"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cache_evicted_by_temporary_data(start_cluster):
    q = node.query
    qi = lambda query: int(node.query(query).strip())

    cache_size_initial = qi("SELECT sum(size) FROM system.filesystem_cache")
    assert cache_size_initial == 0

    free_space_initial = qi(
        "SELECT free_space FROM system.disks WHERE name = 'tiny_local_cache_local_disk'"
    )
    assert free_space_initial > 8 * 1024 * 1024

    q(
        "CREATE TABLE t1 (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'tiny_local_cache'"
    )
    q("INSERT INTO t1 SELECT number FROM numbers(1024 * 1024)")

    # To be sure that nothing is reading the cache and entries for t1 can be evited
    q("OPTIMIZE TABLE t1 FINAL")
    q("SYSTEM STOP MERGES t1")

    # Read some data to fill the cache
    q("SELECT sum(x) FROM t1")

    cache_size_with_t1 = qi("SELECT sum(size) FROM system.filesystem_cache")
    assert cache_size_with_t1 > 8 * 1024 * 1024

    # Almost all disk space is occupied by t1 cache
    free_space_with_t1 = qi(
        "SELECT free_space FROM system.disks WHERE name = 'tiny_local_cache_local_disk'"
    )
    assert free_space_with_t1 < 4 * 1024 * 1024

    # Try to sort the table, but fail because of lack of disk space
    with pytest.raises(QueryRuntimeException) as exc:
        q(
            "SELECT ignore(*) FROM numbers(10 * 1024 * 1024) ORDER BY sipHash64(number)",
            settings={
                "max_bytes_before_external_group_by": "4M",
                "max_bytes_before_external_sort": "4M",
            },
        )
    assert "Failed to reserve space for the file cache" in str(exc.value)

    # Some data evicted from cache by temporary data
    cache_size_after_eviction = qi("SELECT sum(size) FROM system.filesystem_cache")
    assert cache_size_after_eviction < cache_size_with_t1

    # Disk space freed, at least 3 MB, because temporary data tried to write 4 MB
    free_space_after_eviction = qi(
        "SELECT free_space FROM system.disks WHERE name = 'tiny_local_cache_local_disk'"
    )
    assert free_space_after_eviction > free_space_with_t1 + 3 * 1024 * 1024

    q("DROP TABLE IF EXISTS t1")
