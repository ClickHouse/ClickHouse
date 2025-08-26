import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

hot_volume_size_mb = 5
warm_volume_size_mb = 10
cold_volume_size_mb = 15
mb_in_bytes = 1024 * 1024

node_options = dict(
    with_zookeeper=True,
    main_configs=[
        "configs/remote_servers.xml",
        "configs/config.d/storage_configuration.xml",
    ],
    tmpfs=[
        f"/hot:size={hot_volume_size_mb}M",
        f"/warm:size={warm_volume_size_mb}M",
        f"/cold:size={cold_volume_size_mb}M",
    ],
)

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", macros={"shard": 0, "replica": 1}, **node_options)
node2 = cluster.add_instance("node2", macros={"shard": 0, "replica": 2}, **node_options)
nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def wait_until_moves_finished(node, requred_part_count, disk):
    retry_count = 20
    sleep_time = 1
    for _ in range(retry_count):
        try:
            parts_on_disk = int(
                node.query(f"SELECT count() FROM system.parts WHERE disk_name='{disk}'")
            )
            if parts_on_disk <= requred_part_count:
                return True
        except Exception:
            pass
        time.sleep(sleep_time)
    return False


def check_by_insert_time_parts_disks(node, database):
    res = node.query(
        f"SELECT disk_name, toUnixTimestamp(min(min_time_of_data_insert)) AS min_time, toUnixTimestamp(max(min_time_of_data_insert)) AS max_time FROM system.parts WHERE database ='{database}' GROUP BY disk_name"
    )

    times_of_parts = {}
    for line in res.splitlines():
        [disk_name, min_time, max_time] = line.split("\t")
        times_of_parts[disk_name] = (int(min_time), int(max_time))

    # min_time at i disks must be >= max_time at j disk. Where  i > j.
    assert (
        times_of_parts["cold"][0] <= times_of_parts["hot"][1]
        and times_of_parts["cold"][0] <= times_of_parts["warm"][1]
    )
    assert times_of_parts["warm"][0] <= times_of_parts["hot"][1]


@pytest.mark.parametrize(
    "storage_policy,additional_check",
    [
        ("jbod_by_size_policy", None),
        ("jbod_time_policy", check_by_insert_time_parts_disks),
    ],
)
def test_simple_moves(started_cluster, storage_policy, additional_check):
    node = node1

    node.query("DROP DATABASE IF EXISTS test_db SYNC;")
    node.query("CREATE DATABASE test_db;")

    node.query(
        f"CREATE TABLE test_db.table (a Int, b String) ENGINE=MergeTree() ORDER BY a SETTINGS storage_policy='{storage_policy}'"
    )

    node.query(f"SYSTEM STOP MERGES test_db.table;")

    for _ in range(15):
        node.query(
            f"INSERT INTO test_db.table SELECT rand()%10, randomString({mb_in_bytes});"
        )
        time_last_data_insert = int(time.time())
        assert wait_until_moves_finished(node, hot_volume_size_mb // 2, "hot")
        assert wait_until_moves_finished(node, warm_volume_size_mb // 2, "warm")
        # Make sure that times of data inserts are unique
        if int(time.time()) == time_last_data_insert:
            time.sleep(1)

    if additional_check:
        additional_check(node, "test_db")
    node.query(f"DROP DATABASE test_db SYNC;")


@pytest.mark.parametrize(
    "storage_policy,additional_check",
    [
        ("jbod_by_size_policy", None),
        ("jbod_time_policy", check_by_insert_time_parts_disks),
    ],
)
def test_moves_replicated(started_cluster, storage_policy, additional_check):
    node1.query("DROP DATABASE IF EXISTS test_db ON CLUSTER 'test_cluster' SYNC;")
    node1.query("CREATE DATABASE test_db ON CLUSTER 'test_cluster';")
    # Here we need to block merges the execution and scheduling, otherwise parts will be in the `virtual` state
    # and moves of theese parts will be blocked, until merge is completed.
    node1.query(
        f"""
        CREATE TABLE test_db.table ON CLUSTER 'test_cluster' (a Int, b String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{{uuid}}', '{{replica}}') ORDER BY a
        SETTINGS storage_policy='{storage_policy}', max_replicated_merges_in_queue=0;
        """
    )
    node1.query(f"SYSTEM STOP MERGES ON CLUSTER 'test_cluster' test_db.table; ")

    for _ in range(15):
        node1.query(
            f"INSERT INTO test_db.table SELECT rand()%10, randomString({mb_in_bytes});"
        )
        time_last_data_insert = int(time.time())
        assert wait_until_moves_finished(node1, hot_volume_size_mb // 2, "hot")
        assert wait_until_moves_finished(node2, hot_volume_size_mb // 2, "hot")
        assert wait_until_moves_finished(node1, warm_volume_size_mb // 2, "warm")
        assert wait_until_moves_finished(node2, warm_volume_size_mb // 2, "warm")
        if time_last_data_insert == int(time.time()):
            time.sleep(1)

    if additional_check:
        additional_check(node1, "test_db")
        additional_check(node2, "test_db")
    node1.query(f"DROP DATABASE test_db ON CLUSTER 'test_cluster' SYNC;")
