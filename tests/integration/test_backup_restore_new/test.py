import pytest
import re
import os.path
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance", main_configs=["configs/backups_disk.xml"], external_dirs=["/backups/"]
)


def create_and_fill_table(engine="MergeTree", n=100):
    if engine == "MergeTree":
        engine = "MergeTree ORDER BY y PARTITION BY x%10"
    instance.query("CREATE DATABASE test")
    instance.query(f"CREATE TABLE test.table(x UInt32, y String) ENGINE={engine}")
    instance.query(
        f"INSERT INTO test.table SELECT number, toString(number) FROM numbers({n})"
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
        instance.query("DROP DATABASE IF EXISTS test2")
        instance.query("DROP DATABASE IF EXISTS test3")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"


def get_path_to_backup(backup_name):
    name = backup_name.split(",")[1].strip("')/ ")
    return os.path.join(instance.cluster.instances_dir, "backups", name)


session_id_counter = 0


def new_session_id():
    global session_id_counter
    session_id_counter += 1
    return "Session #" + str(session_id_counter)


@pytest.mark.parametrize(
    "engine", ["MergeTree", "Log", "TinyLog", "StripeLog", "Memory"]
)
def test_restore_table(engine):
    backup_name = new_backup_name()
    create_and_fill_table(engine=engine)

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


@pytest.mark.parametrize(
    "engine", ["MergeTree", "Log", "TinyLog", "StripeLog", "Memory"]
)
def test_restore_table_into_existing_table(engine):
    backup_name = new_backup_name()
    create_and_fill_table(engine=engine)

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    expected_error = "already contains some data"
    assert expected_error in instance.query_and_get_error(
        f"RESTORE TABLE test.table FROM {backup_name}"
    )

    instance.query(
        f"RESTORE TABLE test.table FROM {backup_name} SETTINGS structure_only=true"
    )
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    instance.query(
        f"RESTORE TABLE test.table FROM {backup_name} SETTINGS allow_non_empty_tables=true"
    )
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "200\t9900\n"


def test_restore_table_under_another_name():
    backup_name = new_backup_name()
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert instance.query("EXISTS test.table2") == "0\n"

    instance.query(f"RESTORE TABLE test.table AS test.table2 FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table2") == "100\t4950\n"


def test_backup_table_under_another_name():
    backup_name = new_backup_name()
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table AS test.table2 TO {backup_name}")

    assert instance.query("EXISTS test.table2") == "0\n"

    instance.query(f"RESTORE TABLE test.table2 FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table2") == "100\t4950\n"


def test_materialized_view():
    backup_name = new_backup_name()
    instance.query(
        "CREATE MATERIALIZED VIEW mv_1(x UInt8) ENGINE=MergeTree ORDER BY tuple() POPULATE AS SELECT 1 AS x"
    )

    instance.query(f"BACKUP TABLE mv_1 TO {backup_name}")
    instance.query("DROP TABLE mv_1")
    instance.query(f"RESTORE TABLE mv_1 FROM {backup_name}")

    assert instance.query("SELECT * FROM mv_1") == "1\n"
    instance.query("DROP TABLE mv_1")


def test_incremental_backup():
    backup_name = new_backup_name()
    incremental_backup_name = new_backup_name()
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("INSERT INTO test.table VALUES (65, 'a'), (66, 'b')")

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "102\t5081\n"
    instance.query(
        f"BACKUP TABLE test.table TO {incremental_backup_name} SETTINGS base_backup = {backup_name}"
    )

    instance.query(
        f"RESTORE TABLE test.table AS test.table2 FROM {incremental_backup_name}"
    )
    assert instance.query("SELECT count(), sum(x) FROM test.table2") == "102\t5081\n"


def test_incremental_backup_after_renaming_table():
    backup_name = new_backup_name()
    incremental_backup_name = new_backup_name()
    create_and_fill_table()

    instance.query(f"BACKUP TABLE test.table TO {backup_name}")
    instance.query("RENAME TABLE test.table TO test.table2")
    instance.query(
        f"BACKUP TABLE test.table2 TO {incremental_backup_name} SETTINGS base_backup = {backup_name}"
    )

    # Files in a base backup can be searched by checksum, so an incremental backup with a renamed table actually
    # contains only its changed metadata.
    assert (
        os.path.isdir(os.path.join(get_path_to_backup(backup_name), "metadata")) == True
    )
    assert os.path.isdir(os.path.join(get_path_to_backup(backup_name), "data")) == True
    assert (
        os.path.isdir(
            os.path.join(get_path_to_backup(incremental_backup_name), "metadata")
        )
        == True
    )
    assert (
        os.path.isdir(os.path.join(get_path_to_backup(incremental_backup_name), "data"))
        == False
    )

    instance.query("DROP TABLE test.table2")
    instance.query(f"RESTORE TABLE test.table2 FROM {incremental_backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table2") == "100\t4950\n"


def test_backup_not_found_or_already_exists():
    backup_name = new_backup_name()

    expected_error = "Backup .* not found"
    assert re.search(
        expected_error,
        instance.query_and_get_error(
            f"RESTORE TABLE test.table AS test.table2 FROM {backup_name}"
        ),
    )

    create_and_fill_table()
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    expected_error = "Backup .* already exists"
    assert re.search(
        expected_error,
        instance.query_and_get_error(f"BACKUP TABLE test.table TO {backup_name}"),
    )


def test_file_engine():
    backup_name = f"File('/backups/file/')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_database():
    backup_name = new_backup_name()
    create_and_fill_table()
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    instance.query(f"BACKUP DATABASE test TO {backup_name}")
    instance.query("DROP DATABASE test")
    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_zip_archive():
    backup_name = f"Disk('backups', 'archive.zip')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")
    assert os.path.isfile(get_path_to_backup(backup_name))

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_zip_archive_with_settings():
    backup_name = f"Disk('backups', 'archive_with_settings.zip')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(
        f"BACKUP TABLE test.table TO {backup_name} SETTINGS compression_method='lzma', compression_level=3, password='qwerty'"
    )

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(
        f"RESTORE TABLE test.table FROM {backup_name} SETTINGS password='qwerty'"
    )
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_async():
    create_and_fill_table()
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    backup_name = new_backup_name()
    [id, _, status] = instance.query(
        f"BACKUP TABLE test.table TO {backup_name} ASYNC"
    ).split("\t")
    assert status == "MAKING_BACKUP\n" or status == "BACKUP_COMPLETE\n"
    assert_eq_with_retry(
        instance,
        f"SELECT status FROM system.backups WHERE uuid='{id}'",
        "BACKUP_COMPLETE\n",
    )

    instance.query("DROP TABLE test.table")

    [id, _, status] = instance.query(
        f"RESTORE TABLE test.table FROM {backup_name} ASYNC"
    ).split("\t")
    assert status == "RESTORING\n" or status == "RESTORED\n"
    assert_eq_with_retry(
        instance, f"SELECT status FROM system.backups WHERE uuid='{id}'", "RESTORED\n"
    )

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_dependencies():
    create_and_fill_table()
    instance.query("CREATE VIEW test.view AS SELECT x, y AS w FROM test.table")
    instance.query(
        "CREATE DICTIONARY test.dict1(x UInt32, w String) PRIMARY KEY x SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB 'test' TABLE 'view')) LAYOUT(FLAT()) LIFETIME(0)"
    )
    instance.query(
        "CREATE DICTIONARY test.dict2(x UInt32, w String) PRIMARY KEY w SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB 'test' TABLE 'dict1')) LAYOUT(FLAT()) LIFETIME(0)"
    )
    instance.query(
        "CREATE TABLE test.table2(k String, v Int32 DEFAULT dictGet('test.dict2', 'x', k) - 1) ENGINE=MergeTree ORDER BY tuple()"
    )
    instance.query("INSERT INTO test.table2 (k) VALUES ('7'), ('96'), ('124')")
    assert instance.query("SELECT * FROM test.table2 ORDER BY k") == TSV(
        [["124", -1], ["7", 6], ["96", 95]]
    )

    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE test AS test2 TO {backup_name}")

    instance.query("DROP DATABASE test")

    instance.query(f"RESTORE DATABASE test2 AS test3 FROM {backup_name}")

    assert instance.query("SELECT * FROM test3.table2 ORDER BY k") == TSV(
        [["124", -1], ["7", 6], ["96", 95]]
    )
    instance.query("INSERT INTO test3.table2 (k) VALUES  ('63'), ('152'), ('71')")
    assert instance.query("SELECT * FROM test3.table2 ORDER BY k") == TSV(
        [["124", -1], ["152", -1], ["63", 62], ["7", 6], ["71", 70], ["96", 95]]
    )


def test_materialized_view():
    create_and_fill_table(n=5)
    instance.query(
        "CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY tuple() POPULATE AS SELECT y, x FROM test.table"
    )
    instance.query("INSERT INTO test.table VALUES (990, 'a')")

    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE test TO {backup_name}")

    assert sorted(
        os.listdir(os.path.join(get_path_to_backup(backup_name), "metadata/test"))
    ) == ["table.sql", "view.sql"]
    assert sorted(
        os.listdir(os.path.join(get_path_to_backup(backup_name), "data/test"))
    ) == ["table", "view"]
    view_create_query = open(
        os.path.join(get_path_to_backup(backup_name), "metadata/test/view.sql")
    ).read()
    assert view_create_query.startswith("CREATE MATERIALIZED VIEW test.view")
    assert "POPULATE" not in view_create_query

    instance.query("DROP DATABASE test")

    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    instance.query("INSERT INTO test.table VALUES (991, 'b')")

    assert instance.query("SELECT * FROM test.view ORDER BY x") == TSV(
        [["0", 0], ["1", 1], ["2", 2], ["3", 3], ["4", 4], ["a", 990], ["b", 991]]
    )


def test_materialized_view_with_target_table():
    create_and_fill_table(n=5)
    instance.query(
        "CREATE TABLE test.target(x Int64, y String) ENGINE=MergeTree ORDER BY tuple()"
    )
    instance.query(
        "CREATE MATERIALIZED VIEW test.view TO test.target AS SELECT y, x FROM test.table"
    )
    instance.query("INSERT INTO test.table VALUES (990, 'a')")

    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE test TO {backup_name}")

    assert sorted(
        os.listdir(os.path.join(get_path_to_backup(backup_name), "metadata/test"))
    ) == ["table.sql", "target.sql", "view.sql"]
    assert sorted(
        os.listdir(os.path.join(get_path_to_backup(backup_name), "data/test"))
    ) == ["table", "target"]

    instance.query("DROP DATABASE test")

    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    instance.query("INSERT INTO test.table VALUES (991, 'b')")

    assert instance.query("SELECT * FROM test.view ORDER BY x") == TSV(
        [["a", 990], ["b", 991]]
    )


def test_temporary_table():
    session_id = new_session_id()
    instance.http_query(
        "CREATE TEMPORARY TABLE temp_tbl(s String)", params={"session_id": session_id}
    )
    instance.http_query(
        "INSERT INTO temp_tbl VALUES ('q')", params={"session_id": session_id}
    )
    instance.http_query(
        "INSERT INTO temp_tbl VALUES ('w'), ('e')", params={"session_id": session_id}
    )

    backup_name = new_backup_name()
    instance.http_query(
        f"BACKUP TEMPORARY TABLE temp_tbl TO {backup_name}",
        params={"session_id": session_id},
    )

    session_id = new_session_id()
    instance.http_query(
        f"RESTORE TEMPORARY TABLE temp_tbl FROM {backup_name}",
        params={"session_id": session_id},
    )

    assert instance.http_query(
        "SELECT * FROM temp_tbl ORDER BY s", params={"session_id": session_id}
    ) == TSV([["e"], ["q"], ["w"]])


# We allow BACKUP DATABASE _temporary_and_external_tables only if the backup doesn't contain any table.
def test_temporary_tables_database():
    session_id = new_session_id()
    instance.http_query(
        "CREATE TEMPORARY TABLE temp_tbl(s String)", params={"session_id": session_id}
    )

    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE _temporary_and_external_tables TO {backup_name}")

    assert os.listdir(os.path.join(get_path_to_backup(backup_name), "metadata/")) == [
        "_temporary_and_external_tables.sql"  # database metadata only
    ]


def test_system_table():
    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE system.numbers TO {backup_name}")

    assert os.listdir(
        os.path.join(get_path_to_backup(backup_name), "metadata/system")
    ) == ["numbers.sql"]

    assert not os.path.isdir(os.path.join(get_path_to_backup(backup_name), "data"))

    create_query = open(
        os.path.join(get_path_to_backup(backup_name), "metadata/system/numbers.sql")
    ).read()

    assert (
        create_query
        == "CREATE TABLE system.numbers (`number` UInt64) ENGINE = SystemNumbers"
    )

    instance.query(f"RESTORE TABLE system.numbers FROM {backup_name}")


def test_system_database():
    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE system TO {backup_name}")

    assert "numbers.sql" in os.listdir(
        os.path.join(get_path_to_backup(backup_name), "metadata/system")
    )

    create_query = open(
        os.path.join(get_path_to_backup(backup_name), "metadata/system/numbers.sql")
    ).read()

    assert (
        create_query
        == "CREATE TABLE system.numbers (`number` UInt64) ENGINE = SystemNumbers"
    )
