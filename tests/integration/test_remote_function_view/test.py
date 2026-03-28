import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/clusters.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/clusters.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        node2.query(
            """
                CREATE TABLE test_table(
                    APIKey UInt32,
                    CustomAttributeId UInt64,
                    ProfileIDHash UInt64,
                    DeviceIDHash UInt64,
                    Data String)
                ENGINE = SummingMergeTree()
                ORDER BY (APIKey, CustomAttributeId, ProfileIDHash, DeviceIDHash, intHash32(DeviceIDHash))
            """
        )
        yield cluster

    finally:
        cluster.shutdown()


def test_remote(start_cluster):
    assert (
        node1.query(
            "SELECT 1 FROM remote('node2', view(SELECT * FROM default.test_table)) WHERE (APIKey = 137715) AND (CustomAttributeId IN (45, 66)) AND (ProfileIDHash != 0) LIMIT 1"
        )
        == ""
    )


def test_remote_fail(start_cluster):
    assert (
        "Unknown table expression identifier 'default.table_not_exists'"
        in node1.query_and_get_error(
            "SELECT 1 FROM remote('node2', view(SELECT * FROM default.table_not_exists)) WHERE (APIKey = 137715) AND (CustomAttributeId IN (45, 66)) AND (ProfileIDHash != 0) LIMIT 1"
        )
    )
