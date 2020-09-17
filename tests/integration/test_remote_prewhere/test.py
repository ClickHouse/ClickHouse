import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/log_conf.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/log_conf.xml'])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for node in [node1, node2]:
            node.query("""
                CREATE TABLE test_table(
                    APIKey UInt32,
                    CustomAttributeId UInt64,
                    ProfileIDHash UInt64,
                    DeviceIDHash UInt64,
                    Data String)
                ENGINE = SummingMergeTree()
                ORDER BY (APIKey, CustomAttributeId, ProfileIDHash, DeviceIDHash, intHash32(DeviceIDHash))
            """)
        yield cluster

    finally:
        cluster.shutdown()


def test_remote(start_cluster):
    assert node1.query(
        "SELECT 1 FROM remote('node{1,2}', default.test_table) WHERE (APIKey = 137715) AND (CustomAttributeId IN (45, 66)) AND (ProfileIDHash != 0) LIMIT 1") == ""
