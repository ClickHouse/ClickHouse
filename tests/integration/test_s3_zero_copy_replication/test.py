import datetime
import logging
import time

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/s3.xml"],
            macros={"replica": "1"},
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/s3.xml"],
            macros={"replica": "2"},
            with_minio=True,
            with_zookeeper=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def get_large_objects_count(cluster, size=100, folder="data"):
    minio = cluster.minio_client
    counter = 0
    for obj in minio.list_objects(cluster.minio_bucket, "{}/".format(folder)):
        if obj.size is not None and obj.size >= size:
            counter = counter + 1
    return counter


def check_objects_exisis(cluster, object_list, folder="data"):
    minio = cluster.minio_client
    for obj in object_list:
        if obj:
            minio.stat_object(cluster.minio_bucket, "{}/{}".format(folder, obj))


def check_objects_not_exisis(cluster, object_list, folder="data"):
    minio = cluster.minio_client
    for obj in object_list:
        if obj:
            try:
                minio.stat_object(cluster.minio_bucket, "{}/{}".format(folder, obj))
            except Exception as error:
                assert "NoSuchKey" in str(error)
            else:
                assert False, "Object {} should not be exists".format(obj)


def wait_for_large_objects_count(cluster, expected, size=100, timeout=30):
    while timeout > 0:
        if get_large_objects_count(cluster, size=size) == expected:
            return
        timeout -= 1
        time.sleep(1)
    assert get_large_objects_count(cluster, size=size) == expected


def wait_for_active_parts(node, num_expected_parts, table_name, timeout=30):
    deadline = time.monotonic() + timeout
    num_parts = 0
    while time.monotonic() < deadline:
        num_parts_str = node.query(
            "select count() from system.parts where table = '{}' and active".format(
                table_name
            )
        )
        num_parts = int(num_parts_str.strip())
        if num_parts == num_expected_parts:
            return

        time.sleep(0.2)

    assert num_parts == num_expected_parts


# Result of `get_large_objects_count` can be changed in other tests, so run this case at the beginning
@pytest.mark.order(0)
@pytest.mark.parametrize("policy", ["s3"])
def test_s3_zero_copy_replication(cluster, policy):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(
        """
        CREATE TABLE s3_test ON CLUSTER test_cluster (id UInt32, value String)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/s3_test', '{}')
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(
            "{replica}", policy
        )
    )

    node1.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")
    node2.query("SYSTEM SYNC REPLICA s3_test")
    assert (
        node1.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )
    assert (
        node2.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )

    # Based on version 21.x - should be only 1 file with size 100+ (checksums.txt), used by both nodes
    assert get_large_objects_count(cluster) == 1

    node2.query("INSERT INTO s3_test VALUES (2,'data'),(3,'data')")
    node1.query("SYSTEM SYNC REPLICA s3_test")

    assert (
        node2.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"
    )
    assert (
        node1.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"
    )

    # Based on version 21.x - two parts
    wait_for_large_objects_count(cluster, 2)

    node1.query("OPTIMIZE TABLE s3_test FINAL")

    # Based on version 21.x - after merge, two old parts and one merged
    wait_for_large_objects_count(cluster, 3)

    # Based on version 21.x - after cleanup - only one merged part
    wait_for_large_objects_count(cluster, 1, timeout=60)

    node1.query("DROP TABLE IF EXISTS s3_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS s3_test NO DELAY")


def test_s3_zero_copy_on_hybrid_storage(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(
        """
        CREATE TABLE hybrid_test ON CLUSTER test_cluster (id UInt32, value String)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/hybrid_test', '{}')
        ORDER BY id
        SETTINGS storage_policy='hybrid'
        """.format(
            "{replica}"
        )
    )

    node1.query("INSERT INTO hybrid_test VALUES (0,'data'),(1,'data')")
    node2.query("SYSTEM SYNC REPLICA hybrid_test")

    assert (
        node1.query("SELECT * FROM hybrid_test ORDER BY id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )
    assert (
        node2.query("SELECT * FROM hybrid_test ORDER BY id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )

    assert (
        node1.query(
            "SELECT partition_id,disk_name FROM system.parts WHERE table='hybrid_test' FORMAT Values"
        )
        == "('all','default')"
    )
    assert (
        node2.query(
            "SELECT partition_id,disk_name FROM system.parts WHERE table='hybrid_test' FORMAT Values"
        )
        == "('all','default')"
    )

    node1.query("ALTER TABLE hybrid_test MOVE PARTITION ID 'all' TO DISK 's31'")

    assert (
        node1.query(
            "SELECT partition_id,disk_name FROM system.parts WHERE table='hybrid_test' FORMAT Values"
        )
        == "('all','s31')"
    )
    assert (
        node2.query(
            "SELECT partition_id,disk_name FROM system.parts WHERE table='hybrid_test' FORMAT Values"
        )
        == "('all','default')"
    )

    # Total objects in S3
    s3_objects = get_large_objects_count(cluster, size=0)

    node2.query("ALTER TABLE hybrid_test MOVE PARTITION ID 'all' TO DISK 's31'")

    assert (
        node1.query(
            "SELECT partition_id,disk_name FROM system.parts WHERE table='hybrid_test' FORMAT Values"
        )
        == "('all','s31')"
    )
    assert (
        node2.query(
            "SELECT partition_id,disk_name FROM system.parts WHERE table='hybrid_test' FORMAT Values"
        )
        == "('all','s31')"
    )

    # Check that after moving partition on node2 no new obects on s3
    wait_for_large_objects_count(cluster, s3_objects, size=0)

    assert (
        node1.query("SELECT * FROM hybrid_test ORDER BY id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )
    assert (
        node2.query("SELECT * FROM hybrid_test ORDER BY id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )

    node1.query("DROP TABLE IF EXISTS hybrid_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS hybrid_test NO DELAY")


def insert_data_time(node, table, number_of_mb, time, start=0):
    values = ",".join(
        f"({x},{time})"
        for x in range(start, int((1024 * 1024 * number_of_mb) / 8) + start + 1)
    )
    node.query(f"INSERT INTO {table} VALUES {values}")


def insert_large_data(node, table):
    tm = time.mktime((datetime.date.today() - datetime.timedelta(days=7)).timetuple())
    insert_data_time(node, table, 1, tm, 0)
    tm = time.mktime((datetime.date.today() - datetime.timedelta(days=3)).timetuple())
    insert_data_time(node, table, 1, tm, 1024 * 1024)
    tm = time.mktime(datetime.date.today().timetuple())
    insert_data_time(node, table, 10, tm, 1024 * 1024 * 2)


@pytest.mark.parametrize(
    ("storage_policy", "large_data", "iterations"),
    [
        ("tiered", False, 10),
        ("tiered_copy", False, 10),
        ("tiered", True, 3),
        ("tiered_copy", True, 3),
    ],
)
def test_s3_zero_copy_with_ttl_move(cluster, storage_policy, large_data, iterations):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query("DROP TABLE IF EXISTS ttl_move_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS ttl_move_test NO DELAY")

    for i in range(iterations):
        node1.query(
            """
            CREATE TABLE ttl_move_test ON CLUSTER test_cluster (d UInt64, d1 DateTime)
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/ttl_move_test', '{}')
            ORDER BY d
            TTL d1 + INTERVAL 2 DAY TO VOLUME 'external'
            SETTINGS storage_policy='{}'
            """.format(
                "{replica}", storage_policy
            )
        )

        if large_data:
            insert_large_data(node1, "ttl_move_test")
        else:
            node1.query("INSERT INTO ttl_move_test VALUES (10, now() - INTERVAL 3 DAY)")
            node1.query("INSERT INTO ttl_move_test VALUES (11, now() - INTERVAL 1 DAY)")

        node1.query("OPTIMIZE TABLE ttl_move_test FINAL")
        node2.query("SYSTEM SYNC REPLICA ttl_move_test")

        if large_data:
            assert (
                node1.query("SELECT count() FROM ttl_move_test FORMAT Values")
                == "(1572867)"
            )
            assert (
                node2.query("SELECT count() FROM ttl_move_test FORMAT Values")
                == "(1572867)"
            )
        else:
            assert (
                node1.query("SELECT count() FROM ttl_move_test FORMAT Values") == "(2)"
            )
            assert (
                node2.query("SELECT count() FROM ttl_move_test FORMAT Values") == "(2)"
            )
            assert (
                node1.query("SELECT d FROM ttl_move_test ORDER BY d FORMAT Values")
                == "(10),(11)"
            )
            assert (
                node2.query("SELECT d FROM ttl_move_test ORDER BY d FORMAT Values")
                == "(10),(11)"
            )

        node1.query("DROP TABLE IF EXISTS ttl_move_test NO DELAY")
        node2.query("DROP TABLE IF EXISTS ttl_move_test NO DELAY")


@pytest.mark.parametrize(
    ("large_data", "iterations"),
    [
        (False, 10),
        (True, 3),
    ],
)
def test_s3_zero_copy_with_ttl_delete(cluster, large_data, iterations):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query("DROP TABLE IF EXISTS ttl_delete_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS ttl_delete_test NO DELAY")

    for i in range(iterations):
        node1.query(
            """
            CREATE TABLE ttl_delete_test ON CLUSTER test_cluster (d UInt64, d1 DateTime)
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/ttl_delete_test', '{}')
            ORDER BY d
            TTL d1 + INTERVAL 2 DAY
            SETTINGS storage_policy='tiered'
            """.format(
                "{replica}"
            )
        )

        if large_data:
            insert_large_data(node1, "ttl_delete_test")
        else:
            node1.query(
                "INSERT INTO ttl_delete_test VALUES (10, now() - INTERVAL 3 DAY)"
            )
            node1.query(
                "INSERT INTO ttl_delete_test VALUES (11, now() - INTERVAL 1 DAY)"
            )

        node1.query("OPTIMIZE TABLE ttl_delete_test FINAL")

        node1.query("SYSTEM SYNC REPLICA ttl_delete_test")
        node2.query("SYSTEM SYNC REPLICA ttl_delete_test")

        if large_data:
            assert (
                node1.query("SELECT count() FROM ttl_delete_test FORMAT Values")
                == "(1310721)"
            )
            assert (
                node2.query("SELECT count() FROM ttl_delete_test FORMAT Values")
                == "(1310721)"
            )
        else:
            assert (
                node1.query("SELECT count() FROM ttl_delete_test FORMAT Values")
                == "(1)"
            )
            assert (
                node2.query("SELECT count() FROM ttl_delete_test FORMAT Values")
                == "(1)"
            )
            assert (
                node1.query("SELECT d FROM ttl_delete_test ORDER BY d FORMAT Values")
                == "(11)"
            )
            assert (
                node2.query("SELECT d FROM ttl_delete_test ORDER BY d FORMAT Values")
                == "(11)"
            )

        node1.query("DROP TABLE IF EXISTS ttl_delete_test NO DELAY")
        node2.query("DROP TABLE IF EXISTS ttl_delete_test NO DELAY")


def wait_mutations(node, table, seconds):
    time.sleep(1)
    while seconds > 0:
        seconds -= 1
        mutations = node.query(
            f"SELECT count() FROM system.mutations WHERE table='{table}' AND is_done=0"
        )
        if mutations == "0\n":
            return
        time.sleep(1)
    mutations = node.query(
        f"SELECT count() FROM system.mutations WHERE table='{table}' AND is_done=0"
    )
    assert mutations == "0\n"


def s3_zero_copy_unfreeze_base(cluster, unfreeze_query_template):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query("DROP TABLE IF EXISTS unfreeze_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS unfreeze_test NO DELAY")

    node1.query(
        """
        CREATE TABLE unfreeze_test ON CLUSTER test_cluster (d UInt64)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/unfreeze_test', '{}')
        ORDER BY d
        SETTINGS storage_policy='s3'
        """.format(
            "{replica}"
        )
    )

    node1.query("INSERT INTO unfreeze_test VALUES (0)")

    node1.query("ALTER TABLE unfreeze_test FREEZE WITH NAME 'freeze_backup1'")
    node2.query("ALTER TABLE unfreeze_test FREEZE WITH NAME 'freeze_backup2'")
    wait_mutations(node1, "unfreeze_test", 10)
    wait_mutations(node2, "unfreeze_test", 10)

    objects01 = node1.get_backuped_s3_objects("s31", "freeze_backup1")
    objects02 = node2.get_backuped_s3_objects("s31", "freeze_backup2")

    assert objects01 == objects02

    check_objects_exisis(cluster, objects01)

    node1.query("TRUNCATE TABLE unfreeze_test")
    node2.query("SYSTEM SYNC REPLICA unfreeze_test")

    objects11 = node1.get_backuped_s3_objects("s31", "freeze_backup1")
    objects12 = node2.get_backuped_s3_objects("s31", "freeze_backup2")

    assert objects01 == objects11
    assert objects01 == objects12

    check_objects_exisis(cluster, objects11)

    node1.query(f"{unfreeze_query_template} 'freeze_backup1'")
    wait_mutations(node1, "unfreeze_test", 10)

    check_objects_exisis(cluster, objects12)

    node2.query(f"{unfreeze_query_template} 'freeze_backup2'")
    wait_mutations(node2, "unfreeze_test", 10)

    check_objects_not_exisis(cluster, objects12)

    node1.query("DROP TABLE IF EXISTS unfreeze_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS unfreeze_test NO DELAY")


def test_s3_zero_copy_unfreeze_alter(cluster):
    s3_zero_copy_unfreeze_base(cluster, "ALTER TABLE unfreeze_test UNFREEZE WITH NAME")


def test_s3_zero_copy_unfreeze_system(cluster):
    s3_zero_copy_unfreeze_base(cluster, "SYSTEM UNFREEZE WITH NAME")


def s3_zero_copy_drop_detached(cluster, unfreeze_query_template):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query("DROP TABLE IF EXISTS drop_detached_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS drop_detached_test NO DELAY")

    node1.query(
        """
        CREATE TABLE drop_detached_test ON CLUSTER test_cluster (d UInt64)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/drop_detached_test', '{}')
        ORDER BY d PARTITION BY d
        SETTINGS storage_policy='s3'
        """.format(
            "{replica}"
        )
    )

    node1.query("INSERT INTO drop_detached_test VALUES (0)")
    node1.query("ALTER TABLE drop_detached_test FREEZE WITH NAME 'detach_backup1'")
    node1.query("INSERT INTO drop_detached_test VALUES (1)")
    node1.query("ALTER TABLE drop_detached_test FREEZE WITH NAME 'detach_backup2'")
    node2.query("SYSTEM SYNC REPLICA drop_detached_test")

    objects1 = node1.get_backuped_s3_objects("s31", "detach_backup1")
    objects2 = node1.get_backuped_s3_objects("s31", "detach_backup2")

    objects_diff = list(set(objects2) - set(objects1))

    node1.query(f"{unfreeze_query_template} 'detach_backup2'")
    node1.query(f"{unfreeze_query_template} 'detach_backup1'")

    node1.query("ALTER TABLE drop_detached_test DETACH PARTITION '0'")
    node1.query("ALTER TABLE drop_detached_test DETACH PARTITION '1'")
    node2.query("SYSTEM SYNC REPLICA drop_detached_test")

    wait_mutations(node1, "drop_detached_test", 10)
    wait_mutations(node2, "drop_detached_test", 10)

    check_objects_exisis(cluster, objects1)
    check_objects_exisis(cluster, objects2)

    node2.query(
        "ALTER TABLE drop_detached_test DROP DETACHED PARTITION '1'",
        settings={"allow_drop_detached": 1},
    )
    node1.query("SYSTEM SYNC REPLICA drop_detached_test")
    wait_mutations(node1, "drop_detached_test", 10)
    wait_mutations(node2, "drop_detached_test", 10)

    check_objects_exisis(cluster, objects1)
    check_objects_exisis(cluster, objects2)

    node1.query(
        "ALTER TABLE drop_detached_test DROP DETACHED PARTITION '1'",
        settings={"allow_drop_detached": 1},
    )
    node2.query("SYSTEM SYNC REPLICA drop_detached_test")
    wait_mutations(node1, "drop_detached_test", 10)
    wait_mutations(node2, "drop_detached_test", 10)

    check_objects_exisis(cluster, objects1)
    check_objects_not_exisis(cluster, objects_diff)

    node1.query(
        "ALTER TABLE drop_detached_test DROP DETACHED PARTITION '0'",
        settings={"allow_drop_detached": 1},
    )
    node2.query("SYSTEM SYNC REPLICA drop_detached_test")
    wait_mutations(node1, "drop_detached_test", 10)
    wait_mutations(node2, "drop_detached_test", 10)

    check_objects_exisis(cluster, objects1)

    node2.query(
        "ALTER TABLE drop_detached_test DROP DETACHED PARTITION '0'",
        settings={"allow_drop_detached": 1},
    )
    node1.query("SYSTEM SYNC REPLICA drop_detached_test")
    wait_mutations(node1, "drop_detached_test", 10)
    wait_mutations(node2, "drop_detached_test", 10)

    check_objects_not_exisis(cluster, objects1)


def test_s3_zero_copy_drop_detached_alter(cluster):
    s3_zero_copy_drop_detached(
        cluster, "ALTER TABLE drop_detached_test UNFREEZE WITH NAME"
    )


def test_s3_zero_copy_drop_detached_system(cluster):
    s3_zero_copy_drop_detached(cluster, "SYSTEM UNFREEZE WITH NAME")


def test_s3_zero_copy_concurrent_merge(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query("DROP TABLE IF EXISTS concurrent_merge NO DELAY")
    node2.query("DROP TABLE IF EXISTS concurrent_merge NO DELAY")

    for node in (node1, node2):
        node.query(
            """
        CREATE TABLE concurrent_merge (id UInt64)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/concurrent_merge', '{replica}')
        ORDER BY id
        SETTINGS index_granularity=2, storage_policy='s3', remote_fs_execute_merges_on_single_replica_time_threshold=1
        """
        )

    node1.query("system stop merges")
    node2.query("system stop merges")

    # This will generate two parts with 20 granules each
    node1.query("insert into concurrent_merge select number from numbers(40)")
    node1.query("insert into concurrent_merge select number + 1 from numbers(40)")

    wait_for_active_parts(node2, 2, "concurrent_merge")

    # Merge will materialize default column, it should sleep every granule and take 20 * 2 * 0.1 = 4 sec.
    node1.query("alter table concurrent_merge add column x UInt32 default sleep(0.1)")

    node1.query("system start merges")
    node2.query("system start merges")

    # Now, the merge should start.
    # Because of remote_fs_execute_merges_on_single_replica_time_threshold=1,
    # only one replica will start merge instantly.
    # The other replica should wait for 1 sec and also start it.
    # That should probably cause a data race at s3 storage.
    # For now, it does not happen (every blob has a random name, and we just have a duplicating data)
    node1.query("optimize table concurrent_merge final")

    wait_for_active_parts(node1, 1, "concurrent_merge")
    wait_for_active_parts(node2, 1, "concurrent_merge")

    for node in (node1, node2):
        assert node.query("select sum(id) from concurrent_merge").strip() == "1600"
