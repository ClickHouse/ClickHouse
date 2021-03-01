import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from helpers.network import PartitionManager
from helpers.corrupt_part_data_on_disk import corrupt_part_data_by_path

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
        node.query("SYSTEM SYNC REPLICA test")

        for i in range(10):
            assert node.query("SELECT count() FROM test WHERE n % 10 == " + str(i)) == \
                "0\n" if i in detached_parts else "10\n"

        assert node.query("SELECT count() FROM system.parts WHERE table='test'") == \
            str(10 - len(detached_parts)) + "\n"

        # We don't check for system.detached_parts count = len(detached parts) as the newly downloaded data
        # is not removed from detached_parts (so this is wrong)

        res: str = node.query("SELECT * FROM test ORDER BY n")

        for other in nodes:
            if other != node:
                assert_eq_with_retry(other, "SELECT * FROM test ORDER BY n", res)


# 1. Check that ALTER TABLE ATTACH PART|PARTITION does not fetch data from other replicas if it's present in the
# detached/ folder.
# 2. Check that ALTER TABLE ATTACH PART|PARTITION downloads the data from other replicas if the detached/ folder
# does not contain the part with the correct checksums.
def test_attach_without_fetching(start_cluster):
    # Note here requests are used for both PARTITION and PART. This is done for better test diversity.
    # The partition and part are used interchangeably which is not true in most cases.
    # 0. Insert data on two replicas
    node_1.query("INSERT INTO test SELECT * FROM numbers(100)")

    check_data([node_1, node_2], detached_parts=[])

    # 1. Detach the first two partitions/parts on the replicas
    # This part will be fetched from other replicas as it would be missing in the detached/ folder and
    # also attached locally.
    node_1.query("ALTER TABLE test DETACH PART '0_0_0_0'")
    # This partition will be just fetched from other replicas as the checksums won't match
    # (we'll manually break the data).
    node_1.query("ALTER TABLE test DETACH PARTITION 1")

    check_data([node_1, node_2], detached_parts=[0, 1])

    # 2. Create the third replica
    fill_node(node_3)

    # 3. Attach the first part and check if it has been fetched correctly.
    # Replica 3 should download the data from replica 2 as there is no local data.
    # Replica 1 should attach the local data from detached/.
    with PartitionManager() as pm:
        # The non-initiator replica downloads the data from initiator only.
        # If something goes wrong and replica 1 wants to download data from replica 2, the test will fail.
        pm.partition_instances(node_1, node_2)
        node_2.query("ALTER TABLE test ATTACH PART '0_0_0_0'")

    check_data([node_1, node_2, node_3], detached_parts=[1])

    # 4. Break the part data on the second node to corrupt the checksums.
    # Replica 3 should download the data from replica 1 as there is no local data.
    # Replica 2 should also download the data from 1 as the checksums won't match.
    corrupt_part_data_by_path(node_2, "/var/lib/clickhouse/data/default/test/detached/1_0_0_0")
    node_1.query("ALTER TABLE test ATTACH PARTITION 1")
    check_data([node_1, node_2, node_3], detached_parts=[])
