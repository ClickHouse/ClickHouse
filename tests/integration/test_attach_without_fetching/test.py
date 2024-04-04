import time
import pytest
import logging

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from helpers.network import PartitionManager
from helpers.corrupt_part_data_on_disk import corrupt_part_data_by_path


def fill_node(node):
    node.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test(n UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test', '{replica}')
        ORDER BY n PARTITION BY n % 10;
    """.format(
            replica=node.name
        )
    )


cluster = ClickHouseCluster(__file__)

node_1 = cluster.add_instance("replica1", with_zookeeper=True)
node_2 = cluster.add_instance("replica2", with_zookeeper=True)
node_3 = cluster.add_instance("replica3", with_zookeeper=True)


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
        print(
            "> Replication queue for",
            node.name,
            "\n> table\treplica_name\tsource_replica\ttype\tposition\n",
            node.query_with_retry(
                "SELECT table, replica_name, source_replica, type, position FROM system.replication_queue"
            ),
        )

        node.query_with_retry("SYSTEM SYNC REPLICA test")

        print("> Checking data integrity for", node.name)

        for i in range(10):
            assert_eq_with_retry(
                node,
                "SELECT count() FROM test WHERE n % 10 == " + str(i),
                "0\n" if i in detached_parts else "10\n",
            )

        assert_eq_with_retry(
            node,
            "SELECT count() FROM system.parts WHERE table='test'",
            str(10 - len(detached_parts)) + "\n",
        )

        res: str = node.query("SELECT * FROM test ORDER BY n")

        for other in nodes:
            if other != node:
                logging.debug(
                    f"> Checking data consistency, {other.name} vs {node.name}"
                )
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

    # 1.
    # This part will be fetched from other replicas as it would be missing in the detached/ folder and
    # also attached locally.
    node_1.query("ALTER TABLE test DETACH PART '0_0_0_0'")
    # This partition will be just fetched from other replicas as the checksums won't match
    # (we'll manually break the data).
    node_1.query("ALTER TABLE test DETACH PARTITION 1")
    # This partition will be just fetched from other replicas as the part data will be corrupted with one of the
    # files missing.
    node_1.query("ALTER TABLE test DETACH PARTITION 2")

    check_data([node_1, node_2], detached_parts=[0, 1, 2])

    # 2. Create the third replica
    fill_node(node_3)

    # 3. Break the part data on the second node to corrupt the checksums.
    # Replica 3 should download the data from replica 1 as there is no local data.
    # Replica 2 should also download the data from 1 as the checksums won't match.
    logging.debug("Checking attach with corrupted part data with files missing")

    to_delete = node_2.exec_in_container(
        [
            "bash",
            "-c",
            "cd {p} && ls *.bin".format(
                p="/var/lib/clickhouse/data/default/test/detached/2_0_0_0"
            ),
        ],
        privileged=True,
    )
    logging.debug(f"Before deleting: {to_delete}")

    node_2.exec_in_container(
        [
            "bash",
            "-c",
            "cd {p} && rm -fr *.bin".format(
                p="/var/lib/clickhouse/data/default/test/detached/2_0_0_0"
            ),
        ],
        privileged=True,
    )

    node_1.query("ALTER TABLE test ATTACH PARTITION 2")
    check_data([node_1, node_2, node_3], detached_parts=[0, 1])

    # 4. Break the part data on the second node to corrupt the checksums.
    # Replica 3 should download the data from replica 1 as there is no local data.
    # Replica 2 should also download the data from 1 as the checksums won't match.
    print("Checking attach with corrupted part data with all of the files present")

    corrupt_part_data_by_path(
        node_2, "/var/lib/clickhouse/data/default/test/detached/1_0_0_0"
    )

    node_1.query("ALTER TABLE test ATTACH PARTITION 1")
    check_data([node_1, node_2, node_3], detached_parts=[0])

    # 5. Attach the first part and check if it has been fetched correctly.
    # Replica 2 should attach the local data from detached/.
    # Replica 3 should download the data from replica 2 as there is no local data and other connections are broken.
    print("Checking attach with valid checksums")

    with PartitionManager() as pm:
        # If something goes wrong and replica 2 wants to fetch data, the test will fail.
        pm.partition_instances(node_2, node_1, action="REJECT --reject-with tcp-reset")
        pm.partition_instances(node_1, node_3, action="REJECT --reject-with tcp-reset")

        node_1.query("ALTER TABLE test ATTACH PART '0_0_0_0'")

        check_data([node_1, node_2, node_3], detached_parts=[])
