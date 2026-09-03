import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replicated_engine_parse_metadata_on_attach():
    node.query(
        """
        CREATE TABLE data (
            key Int,
            INDEX key_idx0 key+0 TYPE minmax GRANULARITY 1,
            INDEX key_idx1 key+1 TYPE minmax GRANULARITY 1
        )
        ENGINE = ReplicatedMergeTree('/ch/tables/default/data', 'node')
        ORDER BY key;
        """
    )
    node.query("DETACH TABLE data")

    zk = cluster.get_kazoo_client("zoo1")
    # Add **extra space between indices**, to check that it will be re-parsed
    # and successfully accepted by the server.
    #
    # This metadata was obtain from the server without #11325
    zk.set(
        "/ch/tables/default/data/replicas/node/metadata",
        b"""
metadata format version: 1
date column: 
sampling expression: 
index granularity: 8192
mode: 0
sign column: 
primary key: key
data format version: 1
partition key: 
indices:  key_idx0 key + 0 TYPE minmax GRANULARITY 1,  key_idx1 key + 1 TYPE minmax GRANULARITY 1
granularity bytes: 10485760

""".lstrip(),
    )
    node.query("ATTACH TABLE data")
