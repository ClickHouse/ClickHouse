import string
import random

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

from helpers.test_tools import TSV

main_configs = [
    "configs/cluster.xml",
    "configs/cluster3.xml",
    "configs/replicated_access_storage.xml",
    "configs/backups_disk.xml",
    "configs/lesser_timeouts.xml",  # Default timeouts are quite big (a few minutes), the tests don't need them to be that big.
    "configs/data_file_name_generator_from_checksum.xml",
]

user_configs = [
    "configs/allow_database_types.xml",
    "configs/zookeeper_retries.xml",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,  # Necessary for the "test_stop_other_host_while_backup" test
)

node3 = cluster.add_instance(
    "node3",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node3", "shard": "shard1"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


backup_id_counter = 0


@pytest.fixture(autouse=True)
def drop_after_test():
    try:
        yield
    finally:
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP TABLE IF EXISTS tbl2 ON CLUSTER 'cluster3' SYNC")
        node1.query("DROP DATABASE IF EXISTS mydb ON CLUSTER 'cluster3' SYNC")


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def get_path_to_backup(backup_name):
    name = backup_name.split(",")[1].strip("')/ ")
    return os.path.join(node1.cluster.instances_dir, "backups", name)


def test_replicated_table():
    node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (1, 'Don''t')")
    node2.query("INSERT INTO tbl VALUES (2, 'count')")
    node1.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (3, 'your')")
    node2.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (4, 'chickens')")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    backup_name = new_backup_name()

    # Make backup on node 1.
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=1"
    )

    # Drop table on both nodes.
    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node2.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )


def test_empty_replicated_table():
    node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")
    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    backup_name = new_backup_name()

    # Make backup on node 1.
    node1.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=1"
    )

    # Drop table on both nodes.
    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")

    assert node1.query("SELECT * FROM tbl") == ""
    assert node2.query("SELECT * FROM tbl") == ""


def test_replicated_database():
    node1.query("DROP DATABASE IF EXISTS mydb ON CLUSTER 'cluster' SYNC")
    node1.query(
        "CREATE DATABASE mydb ON CLUSTER 'cluster' ENGINE=Replicated('/clickhouse/path/','{shard}','{replica}')"
    )

    node1.query(
        "CREATE TABLE mydb.tbl(x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x"
    )

    node2.query("SYSTEM SYNC DATABASE REPLICA mydb")

    node1.query("INSERT INTO mydb.tbl VALUES (1, 'Don''t')")
    node2.query("INSERT INTO mydb.tbl VALUES (2, 'count')")
    node1.query("INSERT INTO mydb.tbl VALUES (3, 'your')")
    node2.query("INSERT INTO mydb.tbl VALUES (4, 'chickens')")
    node1.query("OPTIMIZE TABLE mydb.tbl ON CLUSTER 'cluster' FINAL")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    # check data in sync
    expect = TSV([[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]])
    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == expect
    assert node2.query("SELECT * FROM mydb.tbl ORDER BY x") == expect

    # Make backup.
    backup_name = new_backup_name()
    node1.query(
        f"BACKUP DATABASE mydb ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica_num=2"
    )

    # Drop table on both nodes.
    node1.query("DROP DATABASE mydb ON CLUSTER 'cluster' SYNC")

    # Restore from backup on node2.
    node1.query(f"RESTORE DATABASE mydb ON CLUSTER 'cluster' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' mydb.tbl")

    assert node1.query("SELECT * FROM mydb.tbl ORDER BY x") == expect
    assert node2.query("SELECT * FROM mydb.tbl ORDER BY x") == expect


def test_replicated_table_restored_into_bigger_cluster():
    for cluster in ["cluster", "cluster3"]:
        node1.query(f"DROP TABLE IF EXISTS tbl ON CLUSTER '{cluster}' SYNC")

    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster3' FROM {backup_name}")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster3' tbl")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])
    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])
    assert node3.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])


def test_replicated_table_restored_into_smaller_cluster():
    for cluster in ["cluster", "cluster1"]:
        node1.query(f"DROP TABLE IF EXISTS tbl ON CLUSTER '{cluster}' SYNC")

    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt32"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    node1.query("INSERT INTO tbl VALUES (111)")
    node2.query("INSERT INTO tbl VALUES (222)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    node1.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster1' FROM {backup_name}")
    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV([111, 222])


def test_projection():
    node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")

    node1.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' (x UInt32, y String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}') "
        "ORDER BY y PARTITION BY x%10"
    )
    node1.query(f"INSERT INTO tbl SELECT number, toString(number) FROM numbers(3)")

    node1.query("ALTER TABLE tbl ADD PROJECTION prjmax (SELECT MAX(x))")
    node1.query(f"INSERT INTO tbl VALUES (100, 'a'), (101, 'b')")

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "2\n"
    )

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    node1.query(f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "0\n"
    )

    node1.query(f"RESTORE TABLE tbl FROM {backup_name}")

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[0, "0"], [1, "1"], [2, "2"], [100, "a"], [101, "b"]]
    )

    assert (
        node1.query(
            "SELECT count() FROM system.projection_parts WHERE database='default' AND table='tbl' AND name='prjmax'"
        )
        == "2\n"
    )


def test_file_deduplication():
    node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")

    # Random column name helps finding it in logs.
    column_name = "".join(random.choice(string.ascii_letters) for x in range(10))

    # Make four replicas in total: 2 on each host.
    node1.query(
        f"""
        CREATE TABLE tbl ON CLUSTER 'cluster' (
        {column_name} Int32
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{{replica}}')
        ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0
        """
    )

    node1.query(
        f"""
        CREATE TABLE tbl2 ON CLUSTER 'cluster' (
        {column_name} Int32
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{{replica}}-2')
        ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0
        """
    )

    # Unique data.
    node1.query(
        f"INSERT INTO tbl VALUES (3556), (1177), (4004), (4264), (3729), (1438), (2158), (2684), (415), (1917)"
    )
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl")
    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster' tbl2")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl, TABLE tbl2 ON CLUSTER 'cluster' TO {backup_name}")

    node1.query("SYSTEM FLUSH LOGS ON CLUSTER 'cluster'")

    # The bin file should be written to the backup once, and skipped three times (because there are four replicas in total).
    bin_file_writing_log_line = (
        f"Writing backup .* from file .*{column_name}.bin (disk default)"
    )
    bin_file_skip_log_line = f"Writing backup .* from file .*{column_name}.bin: skipped"

    num_bin_file_writings = int(node1.count_in_log(bin_file_writing_log_line)) + int(
        node2.count_in_log(bin_file_writing_log_line)
    )
    num_bin_file_skips = int(node1.count_in_log(bin_file_skip_log_line)) + int(
        node2.count_in_log(bin_file_skip_log_line)
    )

    assert num_bin_file_writings == 1
    assert num_bin_file_skips == 3
