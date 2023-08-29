# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest
import fnmatch

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
    get_free_space = lambda: int(
        q(
            "SELECT free_space FROM system.disks WHERE name = 'tiny_local_cache_local_disk'"
        ).strip()
    )
    get_cache_size = lambda: int(
        q("SELECT sum(size) FROM system.filesystem_cache").strip()
    )

    assert get_cache_size() == 0

    assert get_free_space() > 8 * 1024 * 1024

    # Codec is NONE to make cache size predictable
    q(
        "CREATE TABLE t1 (x UInt64 CODEC(NONE), y UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'tiny_local_cache'"
    )
    q("INSERT INTO t1 SELECT number, number FROM numbers(1024 * 1024)")

    # To be sure that nothing is reading the cache and entries for t1 can be evited
    q("OPTIMIZE TABLE t1 FINAL")
    q("SYSTEM STOP MERGES t1")

    # Read some data to fill the cache
    q("SELECT sum(x) FROM t1")

    cache_size_with_t1 = get_cache_size()
    assert cache_size_with_t1 > 8 * 1024 * 1024

    # Almost all disk space is occupied by t1 cache
    free_space_with_t1 = get_free_space()
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
    assert fnmatch.fnmatch(
        str(exc.value), "*Failed to reserve * for temporary file*"
    ), exc.value

    # Some data evicted from cache by temporary data
    cache_size_after_eviction = get_cache_size()
    assert cache_size_after_eviction < cache_size_with_t1

    # Disk space freed, at least 3 MB, because temporary data tried to write 4 MB
    assert get_free_space() > free_space_with_t1 + 3 * 1024 * 1024

    # Read some data to fill the cache again
    q("SELECT avg(y) FROM t1")

    cache_size_with_t1 = get_cache_size()
    assert cache_size_with_t1 > 8 * 1024 * 1024, q(
        "SELECT * FROM system.filesystem_cache FORMAT Vertical"
    )

    # Almost all disk space is occupied by t1 cache
    free_space_with_t1 = get_free_space()
    assert free_space_with_t1 < 4 * 1024 * 1024, q(
        "SELECT * FROM system.disks WHERE name = 'tiny_local_cache_local_disk' FORMAT Vertical"
    )

    node.http_query(
        "SELECT randomPrintableASCII(1024) FROM numbers(8 * 1024) FORMAT TSV",
        params={"buffer_size": 0, "wait_end_of_query": 1},
    )

    assert get_free_space() > free_space_with_t1 + 3 * 1024 * 1024

    # not enough space for buffering 32 MB
    with pytest.raises(Exception) as exc:
        node.http_query(
            "SELECT randomPrintableASCII(1024) FROM numbers(32 * 1024) FORMAT TSV",
            params={"buffer_size": 0, "wait_end_of_query": 1},
        )
    assert fnmatch.fnmatch(
        str(exc.value), "*Failed to reserve * for temporary file*"
    ), exc.value

    q("DROP TABLE IF EXISTS t1")
