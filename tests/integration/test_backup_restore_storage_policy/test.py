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


def create_and_fill_table(n=100):
    instance.query("CREATE DATABASE test")
    instance.query(
        "CREATE TABLE test.table(x UInt32, y String) ENGINE=MergeTree ORDER BY y PARTITION BY x%10"
    )
    instance.query(
        f"INSERT INTO test.table SELECT number, toString(number) FROM numbers({n})"
    )


@pytest.mark.parametrize("policy", ["disks_in_order", "", None])
def test_restore_table(policy):
    backup_name = new_backup_name()
    n = 20
    sum_n = int((n * (n - 1)) / 2)
    expected = f"{n}\t{sum_n}\n"
    
    create_and_fill_table(n)

    assert instance.query("SELECT count(), sum(x) FROM test.table") == expected

    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("DROP TABLE test.table SYNC")

    assert instance.query("EXISTS test.table") == "0\n"

    restore_query = f"RESTORE TABLE test.table FROM {backup_name}"
    if policy is None:
        policy = "default"
    else:
        restore_query += f" SETTINGS storage_policy = '{policy}'"
        if policy == "":
            policy = "default"

    instance.query(restore_query)

    assert instance.query("SELECT count(), sum(x) FROM test.table") == expected

    assert (
        instance.query("SELECT storage_policy FROM system.tables WHERE name='table'")
        == f"{policy}\n"
    )
