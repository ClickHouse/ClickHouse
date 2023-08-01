import pytest
from helpers.cluster import ClickHouseCluster


backup_id_counter = 0

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=["configs/storage_config.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP DATABASE IF EXISTS test")


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('bak', '{backup_id_counter}/')"


def create_and_fill_table():
    instance.query("CREATE DATABASE test")
    instance.query("CREATE TABLE test.table(x UInt32) ENGINE=MergeTree ORDER BY x")
    instance.query(f"INSERT INTO test.table SELECT number FROM numbers(10)")


@pytest.mark.parametrize("policy", ["disks_in_order", "", None])
def test_restore_table(policy):
    backup_name = new_backup_name()
    create_and_fill_table()
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")
    instance.query("DROP TABLE test.table SYNC")
    restore_query = f"RESTORE TABLE test.table FROM {backup_name}"
    if policy is None:
        policy = "default"
    else:
        restore_query += f" SETTINGS storage_policy = '{policy}'"
        if policy == "":
            policy = "default"
    instance.query(restore_query)

    assert (
        instance.query("SELECT storage_policy FROM system.tables WHERE name='table'")
        == f"{policy}\n"
    )
