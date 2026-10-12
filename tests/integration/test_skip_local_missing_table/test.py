import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        # Table exists only on the remote shard (node2). The local shard (node1)
        # has no underlying table — querying it must throw UNKNOWN_TABLE unless
        # skip_unavailable_shards lets us drop the local shard from the query.
        node2.query(
            "CREATE TABLE data (x UInt32) ENGINE = MergeTree() ORDER BY x"
        )
        node2.query("INSERT INTO data VALUES (1), (2), (3)")

        node1.query(
            """
            CREATE TABLE dist (x UInt32)
            ENGINE = Distributed(local_and_remote, currentDatabase(), data)
            """
        )

        yield cluster
    finally:
        cluster.shutdown()


def test_skip_local_shard_when_table_missing(start_cluster):
    # Without skip_unavailable_shards the local shard fails because `data` is
    # absent on node1, and there are no other replicas to fall back to.
    with pytest.raises(QueryRuntimeException, match="UNKNOWN_TABLE"):
        node1.query(
            "SELECT sum(x) FROM dist SETTINGS prefer_localhost_replica = 1"
        )

    # With skip_unavailable_shards the local shard is skipped and we read only
    # from the remote shard (node2), which has the table populated.
    assert (
        node1.query(
            """
            SELECT sum(x) FROM dist
            SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 1
            """
        ).strip()
        == "6"
    )
