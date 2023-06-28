import pytest
import asyncio
import glob
import re
import random
import os.path
from collections import namedtuple
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/backups_disk.xml",
        "configs/no_allocate_block_numbers_in_batch.xml",
    ],
    user_configs=["configs/zookeeper_retries.xml"],
    external_dirs=["/backups/"],
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
        instance.query("DROP USER IF EXISTS u1")
        instance.query("DROP ROLE IF EXISTS r1, r2")
        instance.query("DROP SETTINGS PROFILE IF EXISTS prof1")
        instance.query("DROP ROW POLICY IF EXISTS rowpol1 ON test.table")
        instance.query("DROP QUOTA IF EXISTS q1")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"


def get_path_to_backup(backup_name):
    name = backup_name.split(",")[1].strip("')/ ")
    return os.path.join(instance.cluster.instances_dir, "backups", name)


def find_files_in_backup_folder(backup_name):
    path = get_path_to_backup(backup_name)
    files = [f for f in glob.glob(path + "/**", recursive=True) if os.path.isfile(f)]
    files += [f for f in glob.glob(path + "/.**", recursive=True) if os.path.isfile(f)]
    return files


session_id_counter = 0


def new_session_id():
    global session_id_counter
    session_id_counter += 1
    return "Session #" + str(session_id_counter)


def has_mutation_in_backup(mutation_name, backup_name, database, table):
    return os.path.exists(
        os.path.join(
            get_path_to_backup(backup_name),
            f"data/{database}/{table}/{mutation_name}",
        )
    )


BackupInfo = namedtuple(
    "BackupInfo",
    "name id status error num_files total_size num_entries uncompressed_size compressed_size files_read bytes_read",
)


def get_backup_info_from_system_backups(by_id=None, by_name=None):
    where_condition = "1"
    if by_id:
        where_condition = f"id = '{by_id}'"
    elif by_name:
        where_condition = f"name = '{by_name}'"

    [
        name,
        id,
        status,
        error,
        num_files,
        total_size,
        num_entries,
        uncompressed_size,
        compressed_size,
        files_read,
        bytes_read,
    ] = (
        instance.query(
            f"SELECT name, id, status, error, num_files, total_size, num_entries, uncompressed_size, compressed_size, files_read, bytes_read "
            f"FROM system.backups WHERE {where_condition} LIMIT 1"
        )
        .strip("\n")
        .split("\t")
    )

    num_files = int(num_files)
    total_size = int(total_size)
    num_entries = int(num_entries)
    uncompressed_size = int(uncompressed_size)
    compressed_size = int(compressed_size)
    files_read = int(files_read)
    bytes_read = int(bytes_read)

    return BackupInfo(
        name=name,
        id=id,
        status=status,
        error=error,
        num_files=num_files,
        total_size=total_size,
        num_entries=num_entries,
        uncompressed_size=uncompressed_size,
        compressed_size=compressed_size,
        files_read=files_read,
        bytes_read=bytes_read,
    )


@pytest.mark.parametrize("engine", ["MergeTree"])
def test_restore_table(engine):
    backup_name = new_backup_name()
    create_and_fill_table(engine=engine)

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


@pytest.mark.parametrize("engine", ["MergeTree"])
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


def test_materialized_view_select_1():
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


def test_increment_backup_without_changes():
    backup_name = new_backup_name()
    incremental_backup_name = new_backup_name()

    create_and_fill_table(n=1)
    assert instance.query("SELECT count(), sum(x) FROM test.table") == TSV([["1", "0"]])

    # prepare first backup without base_backup
    id_backup = instance.query(f"BACKUP TABLE test.table TO {backup_name}").split("\t")[
        0
    ]
    backup_info = get_backup_info_from_system_backups(by_id=id_backup)

    assert backup_info.status == "BACKUP_CREATED"
    assert backup_info.error == ""
    assert backup_info.num_files > 0
    assert backup_info.total_size > 0
    assert (
        0 < backup_info.num_entries and backup_info.num_entries <= backup_info.num_files
    )
    assert backup_info.uncompressed_size > 0
    assert backup_info.compressed_size == backup_info.uncompressed_size

    # create second backup without changes based on the first one
    id_backup2 = instance.query(
        f"BACKUP TABLE test.table TO {incremental_backup_name} SETTINGS base_backup = {backup_name}"
    ).split("\t")[0]

    backup2_info = get_backup_info_from_system_backups(by_id=id_backup2)

    assert backup2_info.status == "BACKUP_CREATED"
    assert backup2_info.error == ""
    assert backup2_info.num_files == backup_info.num_files
    assert backup2_info.total_size == backup_info.total_size
    assert backup2_info.num_entries == 0
    assert backup2_info.uncompressed_size > 0
    assert backup2_info.compressed_size == backup2_info.uncompressed_size

    # restore the second backup
    # we expect to see all files in the meta info of the restore and a sum of uncompressed and compressed sizes
    id_restore = instance.query(
        f"RESTORE TABLE test.table AS test.table2 FROM {incremental_backup_name}"
    ).split("\t")[0]

    assert instance.query("SELECT count(), sum(x) FROM test.table2") == TSV(
        [["1", "0"]]
    )

    restore_info = get_backup_info_from_system_backups(by_id=id_restore)

    assert restore_info.status == "RESTORED"
    assert restore_info.error == ""
    assert restore_info.num_files == backup2_info.num_files
    assert restore_info.total_size == backup2_info.total_size
    assert restore_info.num_entries == backup2_info.num_entries
    assert restore_info.uncompressed_size == backup2_info.uncompressed_size
    assert restore_info.compressed_size == backup2_info.compressed_size
    assert restore_info.files_read == backup2_info.num_files
    assert restore_info.bytes_read == backup2_info.total_size


def test_incremental_backup_overflow():
    backup_name = new_backup_name()
    incremental_backup_name = new_backup_name()

    instance.query("CREATE DATABASE test")
    instance.query(
        "CREATE TABLE test.table(y String CODEC(NONE)) ENGINE=MergeTree ORDER BY tuple()"
    )
    # Create a column of 4GB+10K
    instance.query(
        "INSERT INTO test.table SELECT toString(repeat('A', 1024)) FROM numbers((4*1024*1024)+10)"
    )
    # Force one part
    instance.query("OPTIMIZE TABLE test.table FINAL")

    # ensure that the column's size on disk is indeed greater then 4GB
    assert (
        int(
            instance.query(
                "SELECT bytes_on_disk FROM system.parts_columns WHERE active AND database = 'test' AND table = 'table' AND column = 'y'"
            )
        )
        > 4 * 1024 * 1024 * 1024
    )

    instance.query(f"BACKUP TABLE test.table TO {backup_name}")
    instance.query(
        f"BACKUP TABLE test.table TO {incremental_backup_name} SETTINGS base_backup = {backup_name}"
    )

    # And now check that incremental backup does not have any files
    assert os.listdir(os.path.join(get_path_to_backup(incremental_backup_name))) == [
        ".backup"
    ]


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


def test_empty_files_in_backup():
    instance.query("CREATE DATABASE test")
    instance.query(
        "CREATE TABLE test.tbl1(x Array(UInt8)) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0"
    )
    instance.query("INSERT INTO test.tbl1 VALUES ([])")

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.tbl1 TO {backup_name}")

    instance.query("DROP TABLE test.tbl1")
    instance.query(f"RESTORE ALL FROM {backup_name}")

    assert instance.query("SELECT * FROM test.tbl1") == "[]\n"


def test_projection():
    create_and_fill_table(n=3)

    instance.query("ALTER TABLE test.table ADD PROJECTION prjmax (SELECT MAX(x))")
    instance.query(f"INSERT INTO test.table VALUES (100, 'a'), (101, 'b')")

    assert (
        instance.query(
            "SELECT count() FROM system.projection_parts WHERE database='test' AND table='table' AND name='prjmax'"
        )
        == "2\n"
    )

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert os.path.exists(
        os.path.join(
            get_path_to_backup(backup_name), "data/test/table/1_5_5_0/data.bin"
        )
    )

    assert os.path.exists(
        os.path.join(
            get_path_to_backup(backup_name),
            "data/test/table/1_5_5_0/prjmax.proj/data.bin",
        )
    )

    instance.query("DROP TABLE test.table")

    assert (
        instance.query(
            "SELECT count() FROM system.projection_parts WHERE database='test' AND table='table' AND name='prjmax'"
        )
        == "0\n"
    )

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")

    assert instance.query("SELECT * FROM test.table ORDER BY x") == TSV(
        [[0, "0"], [1, "1"], [2, "2"], [100, "a"], [101, "b"]]
    )

    assert (
        instance.query(
            "SELECT count() FROM system.projection_parts WHERE database='test' AND table='table' AND name='prjmax'"
        )
        == "2\n"
    )


def test_backup_partition():
    create_and_fill_table(n=30)

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.table PARTITIONS '1', '4' TO {backup_name}")

    instance.query("DROP TABLE test.table")

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")

    assert instance.query("SELECT * FROM test.table ORDER BY x") == TSV(
        [[1, "1"], [4, "4"], [11, "11"], [14, "14"], [21, "21"], [24, "24"]]
    )


def test_restore_partition():
    create_and_fill_table(n=30)

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("DROP TABLE test.table")

    instance.query(f"RESTORE TABLE test.table PARTITIONS '2', '3' FROM {backup_name}")

    assert instance.query("SELECT * FROM test.table ORDER BY x") == TSV(
        [[2, "2"], [3, "3"], [12, "12"], [13, "13"], [22, "22"], [23, "23"]]
    )


@pytest.mark.parametrize("exclude_system_log_tables", [False, True])
def test_backup_all(exclude_system_log_tables):
    create_and_fill_table()

    session_id = new_session_id()
    instance.http_query(
        "CREATE TEMPORARY TABLE temp_tbl(s String)", params={"session_id": session_id}
    )
    instance.http_query(
        "INSERT INTO temp_tbl VALUES ('q'), ('w'), ('e')",
        params={"session_id": session_id},
    )

    instance.query("CREATE FUNCTION two_and_half AS (x) -> x * 2.5")

    instance.query("CREATE USER u1 IDENTIFIED BY 'qwe123' SETTINGS custom_a = 1")

    backup_name = new_backup_name()

    exclude_from_backup = []
    if exclude_system_log_tables:
        # See the list of log tables in src/Interpreters/SystemLog.cpp
        log_tables = [
            "query_log",
            "query_thread_log",
            "part_log",
            "trace_log",
            "crash_log",
            "text_log",
            "metric_log",
            "filesystem_cache_log",
            "filesystem_read_prefetches_log",
            "asynchronous_metric_log",
            "opentelemetry_span_log",
            "query_views_log",
            "zookeeper_log",
            "session_log",
            "transactions_info_log",
            "processors_profile_log",
            "asynchronous_insert_log",
            "backup_log",
        ]
        exclude_from_backup += ["system." + table_name for table_name in log_tables]

    backup_command = f"BACKUP ALL {'EXCEPT TABLES ' + ','.join(exclude_from_backup) if exclude_from_backup else ''} TO {backup_name}"

    instance.http_query(backup_command, params={"session_id": session_id})

    instance.query("DROP TABLE test.table")
    instance.query("DROP FUNCTION two_and_half")
    instance.query("DROP USER u1")

    restore_settings = []
    if not exclude_system_log_tables:
        restore_settings.append("allow_non_empty_tables=true")
    restore_command = f"RESTORE ALL FROM {backup_name} {'SETTINGS '+ ', '.join(restore_settings) if restore_settings else ''}"

    session_id = new_session_id()
    instance.http_query(
        restore_command, params={"session_id": session_id}, method="POST"
    )

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    assert instance.http_query(
        "SELECT * FROM temp_tbl ORDER BY s", params={"session_id": session_id}
    ) == TSV([["e"], ["q"], ["w"]])

    assert instance.query("SELECT two_and_half(6)") == "15\n"

    assert (
        instance.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH sha256_password SETTINGS custom_a = 1\n"
    )

    instance.query("DROP TABLE test.table")
    instance.query("DROP FUNCTION two_and_half")
    instance.query("DROP USER u1")
