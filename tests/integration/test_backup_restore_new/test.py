import pytest
import re
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


def create_and_fill_table(engine="MergeTree"):
    if engine == "MergeTree":
        engine = "MergeTree ORDER BY y PARTITION BY x%10"
    instance.query("CREATE DATABASE test")
    instance.query(f"CREATE TABLE test.table(x UInt32, y String) ENGINE={engine}")
    instance.query("INSERT INTO test.table SELECT number, toString(number) FROM numbers(100)")


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


backup_id_counter = 0
def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"test-backup-{backup_id_counter}"



@pytest.mark.parametrize("engine", ["MergeTree", "Log", "TinyLog", "StripeLog"])
def test_restore_table(engine):
    backup_name = new_backup_name()
    create_and_fill_table(engine=engine)

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO '{backup_name}'")

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM '{backup_name}'")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


@pytest.mark.parametrize("engine", ["MergeTree", "Log", "TinyLog", "StripeLog"])
def test_restore_table_into_existing_table(engine):
    backup_name = new_backup_name()
    create_and_fill_table(engine=engine)

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO '{backup_name}'")

    instance.query(f"RESTORE TABLE test.table INTO test.table FROM '{backup_name}'")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "200\t9900\n"

    instance.query(f"RESTORE TABLE test.table INTO test.table FROM '{backup_name}'")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "300\t14850\n"


def test_restore_table_under_another_name():
    backup_name = new_backup_name()
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO '{backup_name}'")

    assert instance.query("EXISTS test.table2") == "0\n"

    instance.query(f"RESTORE TABLE test.table INTO test.table2 FROM '{backup_name}'")
    assert instance.query("SELECT count(), sum(x) FROM test.table2") == "100\t4950\n"


def test_backup_table_under_another_name():
    backup_name = new_backup_name()
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table AS test.table2 TO '{backup_name}'")

    assert instance.query("EXISTS test.table2") == "0\n"

    instance.query(f"RESTORE TABLE test.table2 FROM '{backup_name}'")
    assert instance.query("SELECT count(), sum(x) FROM test.table2") == "100\t4950\n"


def test_incremental_backup():
    backup_name = new_backup_name()
    incremental_backup_name = new_backup_name()
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO '{backup_name}'")

    instance.query("INSERT INTO test.table VALUES (65, 'a'), (66, 'b')")

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "102\t5081\n"
    instance.query(f"BACKUP TABLE test.table TO '{incremental_backup_name}' SETTINGS base_backup = '{backup_name}'")

    instance.query(f"RESTORE TABLE test.table AS test.table2 FROM '{incremental_backup_name}'")
    assert instance.query("SELECT count(), sum(x) FROM test.table2") == "102\t5081\n"


def test_backup_not_found_or_already_exists():
    backup_name = new_backup_name()

    expected_error = "Backup .* not found"
    assert re.search(expected_error, instance.query_and_get_error(f"RESTORE TABLE test.table AS test.table2 FROM '{backup_name}'"))

    create_and_fill_table()
    instance.query(f"BACKUP TABLE test.table TO '{backup_name}'")

    expected_error = "Backup .* already exists"
    assert re.search(expected_error, instance.query_and_get_error(f"BACKUP TABLE test.table TO '{backup_name}'"))
