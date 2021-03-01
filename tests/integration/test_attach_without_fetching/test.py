import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.cluster import ClickHouseKiller
from helpers.test_tools import assert_eq_with_retry
from helpers.network import PartitionManager
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk

def fill_node(node):
    node.query(
    '''
        CREATE TABLE test(n UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test', '{replica}')
        ORDER BY n PARTITION BY n % 10;
    '''.format(replica=node.name))

cluster = ClickHouseCluster(__file__)
configs =["configs/remote_servers.xml"]

node_1 = cluster.add_instance('replica1', with_zookeeper=True, main_configs=configs)
node_2 = cluster.add_instance('replica2', with_zookeeper=True, main_configs=configs)
node_3 = cluster.add_instance('replica3', with_zookeeper=True, main_configs=configs)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        fill_node(node_1)
        fill_node(node_2)
        # the third node is filled after the DETACH query
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()

def check_data(nodes, detached_parts):
    for node in nodes:
        for i in range(10):
            assert node.query("SELECT count() FROM test WHERE key % 10 == " + str(i)) ==
                "0\n" if i in detached_parts else "10\n"

        assert node.query("SELECT count() FROM system.parts WHERE table='test'") ==
            str(10 - len(detached_parts)) + "\n"

        assert node.query("SELECT count() FROM system.detached_parts WHERE table='test'") ==
            str(len(detached_parts)) + "\n"

# 1. Check that ALTER TABLE ATTACH PARTITION does not fetch data from other replicas if it's present in the
# detached/ folder.
# 2. Check that ALTER TABLE ATTACH PARTITION downloads the data from other replicas if the detached/ folder
# does not contain the part with the correct checksums.
def test_attach_without_fetching(start_cluster):
    # 0. Insert data on two replicas
    node_1.query("INSERT INTO test SELECT * FROM numbers(100)")

    check_data([node_1, node_2], detached_parts=[])

    # 1. Detach the first three partition on the replicas

    # This part will be fetched from other replicas as it would be missing in the detached/ folder
    node_1.query("ALTER TABLE test DETACH PARTITION '0_0_0_0'")
    # This part will be fetched from other replicas as the checksums won't match (we'll manually break the data).
    node_1.query("ALTER TABLE test DETACH PARTITION '1_0_0_0'")
    # This part will be copied locally and attached without fetch
    node_1.query("ALTER TABLE test DETACH PARTITION '2_0_0_0'")

    check_data([node_1, node_2], detached_parts=[0, 1, 2])

    # 2. Create the third replica
    fill_node(node_3)

    # 3. Attach the first partition and check if it has been fetched correctly
    node_3.query("ALTER TABLE test ATTACH PARTITION '0_0_0_0'")
    check_data([node_1, node_2, node_3], detached_parts=[1, 2])

    # 4. Fetch the second partition to the third replica, break the data to corrupt the checksums,
    # attach it and check if it also was fetched correctly.
    node_3.query("ALTER TABLE test FETCH PARTITION '1_0_0_0' FROM '/clickhouse/tables/test'")
    corrupt_part_data_on_disk(node_3, 'test', '1_0_0_0', is_detached=True)
    node_3.query("ALTER TABLE test ATTACH PARTITION '1_0_0_0'")

    check_data([node_1, node_2, node_3], detached_parts=[2])

    # 5. Fetch the third partition to the third replica, break the network as so the replica won't be able to
    # download the data, attach the partition (and check it has been attached from the local data)
    node_3.query("ALTER TABLE test FETCH PARTITION '2_0_0_0' FROM '/clickhouse/tables/test'")

    with PartitionManager() as pm:
        pm.partition_instances(node_1, node_3)
        pm.partition_instances(node_2, node_3)

        node_3.query("ALTER TABLE test ATTACH PARTITION '2_0_0_0'")

    check_data([node_1, node_2, node_3], detached_parts=[])
