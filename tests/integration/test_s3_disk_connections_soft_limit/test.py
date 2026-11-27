# A read buffer for an object on an S3 disk normally sends one GET for the whole range it has to
# deliver and keeps the connection until the range is consumed. A MergeTree reader keeps one such
# buffer per substream of every part and consumes them in lockstep, so a merge of parts with many
# substreams (a `JSON` column, for example) holds thousands of connections that are idle nearly all
# the time, and a few merges can take the whole connection group of the disks away from everyone
# else (https://github.com/ClickHouse/ClickHouse/issues/119660).
#
# Above the soft limit of the group, each buffer fill is a separate request instead, so a connection
# is held only while a fill is in flight. This test compares a node whose soft limit is reached at
# once with a node with default limits: both must read the same data, and the limited node must
# issue many more GET requests, one per fill, for the same query and for the same merge.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/s3.xml"], with_minio=True)
node_limited = cluster.add_instance(
    "node_limited", main_configs=["configs/s3.xml", "configs/soft_limit.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_object_requests(instance, query_id):
    instance.query("SYSTEM FLUSH LOGS query_log")
    return int(instance.query(f"""
            SELECT ProfileEvents['DiskS3GetObject']
            FROM system.query_log
            WHERE current_database = currentDatabase() AND query_id = '{query_id}' AND type = 'QueryFinish'
            """))


def get_total_object_requests(instance):
    # Merges read through the threadpool reader, whose threads are not covered by the merge's own
    # profile counters in `system.part_log`, so the server-wide counter is used instead.
    return int(
        instance.query(
            "SELECT sum(value) FROM system.events WHERE event = 'DiskS3GetObject'"
        )
    )


def test_reads_above_soft_limit_do_not_hold_connections(started_cluster):
    checksum_query = "SELECT sum(cityHash64(*)) FROM t"
    settings = {"max_read_buffer_size_remote_fs": 65536, "max_threads": 4}

    for instance in [node, node_limited]:
        instance.query("DROP TABLE IF EXISTS t SYNC")
        instance.query("""
            CREATE TABLE t (key UInt64, a String, b String, c Array(UInt32))
            ENGINE = MergeTree ORDER BY key
            SETTINGS storage_policy = 's3', min_bytes_for_wide_part = 0
            """)
        # Two parts of deterministic, poorly compressible data, so that every `.bin` file spans
        # many buffer fills and both nodes store the same rows.
        for part in range(2):
            instance.query(f"""
                INSERT INTO t
                SELECT
                    number,
                    hex(cityHash64(number)) || hex(cityHash64(number + 1)),
                    hex(sipHash64(number)),
                    arrayMap(x -> cityHash64(number, x), range(4))
                FROM numbers({part} * 1000000, 1000000)
                """)

    checksum = node.query(checksum_query, settings=settings)
    assert node_limited.query(checksum_query, settings=settings) == checksum

    requests = {}
    for instance in [node, node_limited]:
        query_id = f"read_{instance.name}"
        assert (
            instance.query(checksum_query, settings=settings, query_id=query_id)
            == checksum
        )
        requests[instance.name] = get_object_requests(instance, query_id)

    # The limited node fetches every 64 KiB of every stream with its own request.
    assert requests["node_limited"] >= 5 * requests["node"], requests

    for instance in [node, node_limited]:
        requests_before = get_total_object_requests(instance)
        instance.query("OPTIMIZE TABLE t FINAL")
        requests[instance.name] = get_total_object_requests(instance) - requests_before
        assert (
            instance.query(
                "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active"
            )
            == "1\n"
        )
        assert instance.query(checksum_query, settings=settings) == checksum

    # The merge reads whole `.bin` files: one request per file on the default node, one per
    # buffer fill of 1 MiB on the limited node.
    assert requests["node_limited"] >= 5 * requests["node"], requests
