import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)

# Read the data through `ReadBufferFromFileDescriptor`, where the failpoint corrupts it, bypassing every cache.
BUFFERED_READ_SETTINGS = {
    "local_filesystem_read_method": "pread",
    "max_threads": 1,
    "use_uncompressed_cache": 0,
    "use_page_cache_for_local_disks": 0,
    "min_bytes_to_use_direct_io": 0,
}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_part_check_after_read_error_reads_with_direct_io(started_cluster):
    node.query(
        """
        CREATE TABLE t (x UInt64, s String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', 'r1') ORDER BY x
        SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0
        """
    )
    node.query("SYSTEM STOP MERGES t")
    node.query("INSERT INTO t SELECT number, toString(number) FROM numbers(100000)")

    node.query("CHECK TABLE t", query_id="check_part_direct_io")
    node.query("SYSTEM FLUSH LOGS query_log")
    direct_io_failed = node.query(
        "SELECT ProfileEvents['CreatedReadBufferDirectIOFailed'] FROM system.query_log "
        "WHERE query_id = 'check_part_direct_io' AND type = 'QueryFinish'"
    )
    if int(direct_io_failed) > 0:
        pytest.skip("The filesystem does not support O_DIRECT")

    # Loads the marks, so that the failing read below is a read of the column data.
    query = "SELECT sum(x), sum(length(s)) FROM t"
    expected = node.query(query, settings=BUFFERED_READ_SETTINGS)

    node.query("SYSTEM ENABLE FAILPOINT file_read_corrupt_data_without_direct_io")
    try:
        with pytest.raises(QueryRuntimeException, match="CHECKSUM_DOESNT_MATCH"):
            node.query(query, settings=BUFFERED_READ_SETTINGS)

        assert_eq_with_retry(
            node,
            "SELECT value FROM system.events WHERE event = 'PartCheckPassedAfterPageCacheReadError'",
            "1",
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT file_read_corrupt_data_without_direct_io")

    assert node.query("SELECT name FROM system.parts WHERE table = 't' AND active") == "all_0_0_0\n"
    assert node.query("SELECT count() FROM system.detached_parts WHERE table = 't'") == "0\n"
    assert (
        node.query(
            "SELECT count() FROM system.zookeeper "
            "WHERE path = '/clickhouse/tables/t/replicas/r1/parts' AND name = 'all_0_0_0'"
        )
        == "1\n"
    )
    assert node.contains_in_log(
        "Part all_0_0_0 was checked after a read error and is fine when read with O_DIRECT"
    )
    assert node.query(query, settings=BUFFERED_READ_SETTINGS) == expected
