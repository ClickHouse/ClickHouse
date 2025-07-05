from dataclasses import dataclass
import glob
import os.path
import random
import re
import sys
import uuid
from collections import namedtuple
from typing import Dict
from datetime import datetime

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry, wait_condition

script_dir = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/backups_disk.xml"],
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
        instance.query("DROP USER IF EXISTS u1, u2")
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


def has_mutation_in_backup(mutation_id, backup_name, database, table):
    return os.path.exists(
        os.path.join(
            get_path_to_backup(backup_name),
            f"data/{database}/{table}/mutations/{mutation_id}.txt",
        )
    )


def get_events_for_query(query_id: str) -> Dict[str, int]:
    events = TSV(
        instance.query(
            f"""
            SYSTEM FLUSH LOGS;

            WITH arrayJoin(ProfileEvents) as pe
            SELECT pe.1, pe.2
            FROM system.query_log
            WHERE query_id = '{query_id}'
            """
        )
    )
    result = {
        event: int(value)
        for event, value in [line.split("\t") for line in events.lines]
    }
    result["query_id"] = query_id
    return result


@dataclass
class BackupInfo:
    name: str
    id: int
    start_time: datetime
    end_time: datetime
    status: str
    error: str
    num_files: int
    total_size: int
    num_entries: int
    uncompressed_size: int
    compressed_size: int
    files_read: int
    bytes_read: int


def get_backup_info_from_system_backups(by_id=None, by_name=None):
    where_condition = "1"
    if by_id:
        where_condition = f"id = '{by_id}'"
    elif by_name:
        where_condition = f"name = '{by_name}'"

    [
        name,
        id,
        start_time,
        end_time,
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
            f"SELECT name, id, start_time, end_time, status, error, num_files, total_size, num_entries, uncompressed_size, compressed_size, files_read, bytes_read "
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
        start_time=start_time,
        end_time=end_time,
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


def test_restore_materialized_view_with_definer():
    instance.query("CREATE DATABASE test")
    instance.query(
        "CREATE TABLE test.test_table (s String) ENGINE = MergeTree ORDER BY s"
    )
    instance.query("CREATE USER u1")
    instance.query("GRANT SELECT ON *.* TO u1")
    instance.query("GRANT INSERT ON *.* TO u1")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.test_mv_1 (s String)
        ENGINE = MergeTree ORDER BY s
        DEFINER = u1 SQL SECURITY DEFINER
        AS SELECT * FROM test.test_table
        """
    )

    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE test TO {backup_name}")
    instance.query("DROP DATABASE test")
    instance.query("DROP USER u1")

    instance.query(f"RESTORE DATABASE test FROM {backup_name}")


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


def test_incremental_backup_after_settings_change():
    def get_table_ddl(tablename: str):
        """
        Return single-line DDL
        """
        ddl = instance.query(
            f"SELECT create_table_query FROM system.tables WHERE name = '{tablename}' AND database = 'test' SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1, show_create_query_identifier_quoting_rule='when_necessary', show_create_query_identifier_quoting_style='Backticks'"
        )
        return re.sub(
            r"[\\\n]", "", ddl
        )  # remove quoted backslash and the last end of line, the same format in test/table.sql file in a backup

    backup_name = new_backup_name()
    increment_backup_name = new_backup_name()
    increment_backup_name2 = new_backup_name()
    create_and_fill_table(n=1)

    assert instance.query("SELECT count(), sum(x) FROM test.table") == TSV([["1", "0"]])
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    metadata_path = os.path.join(
        get_path_to_backup(backup_name), "metadata/test/table.sql"
    )

    with open(metadata_path) as metadata:
        ddl = metadata.read()

        assert ddl == get_table_ddl("table")

    instance.query("ALTER TABLE test.table ADD COLUMN new_col String")

    instance.query(
        f"BACKUP TABLE test.table TO {increment_backup_name} SETTINGS base_backup = {backup_name}"
    )

    increment_backup_metadata_path = os.path.join(
        get_path_to_backup(increment_backup_name), "metadata/test/table.sql"
    )

    with open(increment_backup_metadata_path) as metadata:
        ddl = metadata.read()
        assert ddl == get_table_ddl("table")

    instance.query(
        "ALTER TABLE test.table MODIFY SETTING non_replicated_deduplication_window = 0"
    )

    instance.query(
        f"BACKUP TABLE test.table TO {increment_backup_name2} SETTINGS base_backup = {increment_backup_name}"
    )

    increment_backup_metadata_path2 = os.path.join(
        get_path_to_backup(increment_backup_name2), "metadata/test/table.sql"
    )

    with open(increment_backup_metadata_path2) as metadata:
        ddl = metadata.read()
        # If checksums for the first part of the files are equal only the diff will be written
        assert ddl == ", non_replicated_deduplication_window = 0"


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
    assert backup_info.start_time < backup_info.end_time
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
    assert backup2_info.start_time > backup_info.end_time
    assert backup2_info.start_time < backup2_info.end_time
    assert backup2_info.num_entries == 0
    assert backup2_info.uncompressed_size > 0
    assert backup2_info.compressed_size == backup2_info.uncompressed_size

    # restore the second backup
    # we expect to see all files in the meta info of the restore and a sum of uncompressed and compressed sizes
    restore_query_id = uuid.uuid4().hex
    id_restore = instance.query(
        f"RESTORE TABLE test.table AS test.table2 FROM {incremental_backup_name}",
        query_id=restore_query_id,
    ).split("\t")[0]

    assert instance.query("SELECT count(), sum(x) FROM test.table2") == TSV(
        [["1", "0"]]
    )

    restore_info = get_backup_info_from_system_backups(by_id=id_restore)
    restore_events = get_events_for_query(restore_query_id)

    assert restore_info.status == "RESTORED"
    assert restore_info.error == ""
    assert restore_info.num_files == backup2_info.num_files
    assert restore_info.total_size == backup2_info.total_size
    assert restore_info.num_entries == backup2_info.num_entries
    assert restore_info.start_time < restore_info.end_time
    assert restore_info.uncompressed_size == backup2_info.uncompressed_size
    assert restore_info.compressed_size == backup2_info.compressed_size
    assert (
        restore_info.files_read + restore_events["RestorePartsSkippedFiles"]
        == backup2_info.num_files
    )
    assert (
        restore_info.bytes_read + restore_events["RestorePartsSkippedBytes"]
        == backup2_info.total_size
    )


def test_incremental_backup_overflow():
    if (
        instance.is_built_with_thread_sanitizer()
        or instance.is_built_with_memory_sanitizer()
        or instance.is_built_with_address_sanitizer()
    ):
        pytest.skip("The test is slow in builds with sanitizer")

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


def test_incremental_backup_for_log_family():
    backup_name = new_backup_name()
    create_and_fill_table(engine="Log")

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("INSERT INTO test.table VALUES (65, 'a'), (66, 'b')")

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "102\t5081\n"

    backup_name2 = new_backup_name()
    instance.query(f"BACKUP TABLE test.table TO {backup_name2}")

    backup_name_inc = new_backup_name()
    instance.query(
        f"BACKUP TABLE test.table TO {backup_name_inc} SETTINGS base_backup = {backup_name}"
    )

    metadata_path = os.path.join(
        get_path_to_backup(backup_name), "metadata/test/table.sql"
    )

    metadata_path2 = os.path.join(
        get_path_to_backup(backup_name2), "metadata/test/table.sql"
    )

    metadata_path_inc = os.path.join(
        get_path_to_backup(backup_name_inc), "metadata/test/table.sql"
    )

    assert os.path.isfile(metadata_path)
    assert os.path.isfile(metadata_path2)
    assert not os.path.isfile(metadata_path_inc)
    assert os.path.getsize(metadata_path) > 0
    assert os.path.getsize(metadata_path) == os.path.getsize(metadata_path2)

    x_bin_path = os.path.join(get_path_to_backup(backup_name), "data/test/table/x.bin")
    y_bin_path = os.path.join(get_path_to_backup(backup_name), "data/test/table/y.bin")

    x_bin_path2 = os.path.join(
        get_path_to_backup(backup_name2), "data/test/table/x.bin"
    )
    y_bin_path2 = os.path.join(
        get_path_to_backup(backup_name2), "data/test/table/y.bin"
    )

    x_bin_path_inc = os.path.join(
        get_path_to_backup(backup_name_inc), "data/test/table/x.bin"
    )

    y_bin_path_inc = os.path.join(
        get_path_to_backup(backup_name_inc), "data/test/table/y.bin"
    )

    assert os.path.isfile(x_bin_path)
    assert os.path.isfile(y_bin_path)
    assert os.path.isfile(x_bin_path2)
    assert os.path.isfile(y_bin_path2)
    assert os.path.isfile(x_bin_path_inc)
    assert os.path.isfile(y_bin_path_inc)

    x_bin_size = os.path.getsize(x_bin_path)
    y_bin_size = os.path.getsize(y_bin_path)
    x_bin_size2 = os.path.getsize(x_bin_path2)
    y_bin_size2 = os.path.getsize(y_bin_path2)
    x_bin_size_inc = os.path.getsize(x_bin_path_inc)
    y_bin_size_inc = os.path.getsize(y_bin_path_inc)

    assert x_bin_size > 0
    assert y_bin_size > 0
    assert x_bin_size2 > 0
    assert y_bin_size2 > 0
    assert x_bin_size_inc > 0
    assert y_bin_size_inc > 0
    assert x_bin_size2 == x_bin_size + x_bin_size_inc
    assert y_bin_size2 == y_bin_size + y_bin_size_inc

    instance.query(f"RESTORE TABLE test.table AS test.table2 FROM {backup_name_inc}")

    assert instance.query("SELECT count(), sum(x) FROM test.table2") == "102\t5081\n"


def test_incremental_backup_append_table_def():
    backup_name = new_backup_name()
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("ALTER TABLE test.table MODIFY SETTING parts_to_throw_insert=100")

    incremental_backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE test.table TO {incremental_backup_name} SETTINGS base_backup = {backup_name}"
    )

    instance.query("DROP TABLE test.table")
    instance.query(f"RESTORE TABLE test.table FROM {incremental_backup_name}")

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    assert "parts_to_throw_insert = 100" in instance.query(
        "SHOW CREATE TABLE test.table"
    )


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
    id = uuid.uuid4()
    backup_name = f"File('/backups/file/{id}/')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query("DROP TABLE test.table")


def test_database():
    backup_name = new_backup_name()
    create_and_fill_table()
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    instance.query(f"BACKUP DATABASE test TO {backup_name}")

    instance.query("DROP DATABASE test")
    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_zip_archive():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_{id}.zip')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert os.path.isfile(get_path_to_backup(backup_name))

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query("DROP TABLE test.table")


def test_zip_archive_with_settings():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_with_settings_{id}.zip')"
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
    instance.query("DROP TABLE test.table")


def test_zip_archive_with_bad_compression_method():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_with_bad_compression_method_{id}.zip')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    expected_error = "Unknown compression method specified for a zip archive"
    assert expected_error in instance.query_and_get_error(
        f"BACKUP TABLE test.table TO {backup_name} SETTINGS id='archive_with_bad_compression_method', compression_method='foobar'"
    )
    assert (
        instance.query(
            "SELECT status FROM system.backups WHERE id='archive_with_bad_compression_method'"
        )
        == "BACKUP_FAILED\n"
    )


def test_tar_archive():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_{id}.tar')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert os.path.isfile(get_path_to_backup(backup_name))

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query("DROP TABLE test.table")


def test_tar_bz2_archive():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_{id}.tar.bz2')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert os.path.isfile(get_path_to_backup(backup_name))

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_tar_gz_archive():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_{id}.tar.gz')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert os.path.isfile(get_path_to_backup(backup_name))

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_tar_lzma_archive():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_{id}.tar.lzma')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert os.path.isfile(get_path_to_backup(backup_name))

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_tar_zst_archive():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_{id}.tar.zst')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert os.path.isfile(get_path_to_backup(backup_name))

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_tar_xz_archive():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_{id}.tar.xz')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert os.path.isfile(get_path_to_backup(backup_name))

    instance.query("DROP TABLE test.table")
    assert instance.query("EXISTS test.table") == "0\n"

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


def test_tar_archive_with_password():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_with_password_{id}.tar')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    expected_error = "Setting a password is not currently supported for libarchive"
    assert expected_error in instance.query_and_get_error(
        f"BACKUP TABLE test.table TO {backup_name} SETTINGS id='tar_archive_with_password', password='password123'"
    )
    assert (
        instance.query(
            "SELECT status FROM system.backups WHERE id='tar_archive_with_password'"
        )
        == "BACKUP_FAILED\n"
    )


def test_tar_archive_with_bad_compression_method():
    id = uuid.uuid4()
    backup_name = f"Disk('backups', 'archive_with_bad_compression_method_{id}.tar')"
    create_and_fill_table()

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    expected_error = "Using compression_method and compression_level options are not supported for tar archives"
    assert expected_error in instance.query_and_get_error(
        f"BACKUP TABLE test.table TO {backup_name} SETTINGS id='tar_archive_with_bad_compression_method', compression_method='foobar'"
    )
    assert (
        instance.query(
            "SELECT status FROM system.backups WHERE id='tar_archive_with_bad_compression_method'"
        )
        == "BACKUP_FAILED\n"
    )


def test_async():
    create_and_fill_table()
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"

    backup_name = new_backup_name()
    [id, status] = instance.query(
        f"BACKUP TABLE test.table TO {backup_name} ASYNC"
    ).split("\t")

    assert status == "CREATING_BACKUP\n" or status == "BACKUP_CREATED\n"

    assert_eq_with_retry(
        instance,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["BACKUP_CREATED", ""]]),
    )

    instance.query("DROP TABLE test.table")

    [id, status] = instance.query(
        f"RESTORE TABLE test.table FROM {backup_name} ASYNC"
    ).split("\t")

    assert status == "RESTORING\n" or status == "RESTORED\n"

    assert_eq_with_retry(
        instance,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["RESTORED", ""]]),
    )

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


@pytest.mark.parametrize("interface", ["native", "http"])
def test_async_backups_to_same_destination(interface):
    create_and_fill_table()
    backup_name = new_backup_name()

    # The first backup.
    if interface == "http":
        res = instance.http_query(f"BACKUP TABLE test.table TO {backup_name} ASYNC")
    else:
        res = instance.query(f"BACKUP TABLE test.table TO {backup_name} ASYNC")
    id1 = res.split("\t")[0]

    # The second backup to the same destination.
    if interface == "http":
        res, err = instance.http_query_and_get_answer_with_error(
            f"BACKUP TABLE test.table TO {backup_name} ASYNC"
        )
    else:
        res, err = instance.query_and_get_answer_with_error(
            f"BACKUP TABLE test.table TO {backup_name} ASYNC"
        )

    # One of those two backups to the same destination is expected to fail.
    # If the second backup is going to fail it can fail either immediately or after a while.
    # If it fails immediately we won't even get its ID.
    id2 = None if err else res.split("\t")[0]

    ids = [id1]
    if id2:
        ids.append(id2)
    ids_for_query = "[" + ", ".join(f"'{id}'" for id in ids) + "]"

    assert_eq_with_retry(
        instance,
        f"SELECT status FROM system.backups WHERE id IN {ids_for_query} AND status == 'CREATING_BACKUP'",
        "",
    )

    ids_succeeded = instance.query(
        f"SELECT id FROM system.backups WHERE id IN {ids_for_query} AND status == 'BACKUP_CREATED'"
    ).splitlines()

    ids_failed = instance.query(
        f"SELECT id FROM system.backups WHERE id IN {ids_for_query} AND status == 'BACKUP_FAILED'"
    ).splitlines()

    assert len(ids_succeeded) == 1
    assert set(ids_succeeded + ids_failed) == set(ids)

    # Check that the first backup is all right.
    instance.query("DROP TABLE test.table")
    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")
    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"


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


# The backup created by "BACKUP DATABASE _temporary_and_external_tables" must not contain tables from other sessions.
def test_temporary_database():
    session_id = new_session_id()
    instance.http_query(
        "CREATE TEMPORARY TABLE temp_tbl(s String)", params={"session_id": session_id}
    )

    other_session_id = new_session_id()
    instance.http_query(
        "CREATE TEMPORARY TABLE other_temp_tbl(s String)",
        params={"session_id": other_session_id},
    )

    backup_name = new_backup_name()
    instance.http_query(
        f"BACKUP DATABASE _temporary_and_external_tables TO {backup_name}",
        params={"session_id": session_id},
    )

    assert os.listdir(
        os.path.join(get_path_to_backup(backup_name), "temporary_tables/metadata")
    ) == ["temp_tbl.sql"]

    assert sorted(os.listdir(get_path_to_backup(backup_name))) == [
        ".backup",
        "temporary_tables",
    ]


def test_restore_all_restores_temporary_tables():
    session_id = new_session_id()
    instance.http_query(
        "CREATE TEMPORARY TABLE temp_tbl(s String)", params={"session_id": session_id}
    )
    instance.http_query(
        "INSERT INTO temp_tbl VALUES ('q'), ('w'), ('e')",
        params={"session_id": session_id},
    )

    backup_name = new_backup_name()
    instance.http_query(
        f"BACKUP TEMPORARY TABLE temp_tbl TO {backup_name}",
        params={"session_id": session_id},
    )

    session_id = new_session_id()
    instance.http_query(
        f"RESTORE ALL FROM {backup_name}",
        params={"session_id": session_id},
        method="POST",
    )

    assert instance.http_query(
        "SELECT * FROM temp_tbl ORDER BY s", params={"session_id": session_id}
    ) == TSV([["e"], ["q"], ["w"]])


def test_required_privileges():
    create_and_fill_table(n=5)

    instance.query("CREATE USER u1")

    backup_name = new_backup_name()
    expected_error = "necessary to have the grant BACKUP ON test.`table`"
    assert expected_error in instance.query_and_get_error(
        f"BACKUP TABLE test.table TO {backup_name}", user="u1"
    )

    instance.query("GRANT BACKUP ON test.table TO u1")
    instance.query(f"BACKUP TABLE test.table TO {backup_name}", user="u1")

    expected_error = "necessary to have the grant INSERT, CREATE TABLE ON test.`table`"
    assert expected_error in instance.query_and_get_error(
        f"RESTORE TABLE test.table FROM {backup_name}", user="u1"
    )

    expected_error = "necessary to have the grant INSERT, CREATE TABLE ON test.table2"
    assert expected_error in instance.query_and_get_error(
        f"RESTORE TABLE test.table AS test.table2 FROM {backup_name}", user="u1"
    )

    instance.query("GRANT INSERT, CREATE ON test.table2 TO u1")
    instance.query("GRANT TABLE ENGINE ON MergeTree TO u1")
    instance.query(
        f"RESTORE TABLE test.table AS test.table2 FROM {backup_name}", user="u1"
    )

    instance.query("DROP TABLE test.table")

    expected_error = "necessary to have the grant INSERT, CREATE TABLE ON test.`table`"
    assert expected_error in instance.query_and_get_error(
        f"RESTORE ALL FROM {backup_name}", user="u1"
    )

    instance.query("GRANT INSERT, CREATE ON test.table TO u1")
    instance.query(f"RESTORE ALL FROM {backup_name}", user="u1")


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

    assert create_query == "CREATE TABLE system.numbers ENGINE = SystemNumbers"

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

    assert create_query == "CREATE TABLE system.numbers ENGINE = SystemNumbers"


def test_system_users():
    instance.query(
        "CREATE USER u1 IDENTIFIED BY 'qwe123' SETTINGS PROFILE 'default', custom_a = 1"
    )
    instance.query("GRANT SELECT ON test.* TO u1")

    instance.query("CREATE ROLE r1, r2")
    instance.query("GRANT r1 TO r2 WITH ADMIN OPTION")
    instance.query("GRANT r2 TO u1")

    instance.query("CREATE SETTINGS PROFILE `prof1` SETTINGS custom_b=2 TO u1")
    instance.query("CREATE ROW POLICY rowpol1 ON test.table USING x<50 TO u1")
    instance.query("CREATE QUOTA q1 TO r1")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas TO {backup_name}"
    )

    instance.query("DROP USER u1")
    instance.query("DROP ROLE r1, r2")
    instance.query("DROP SETTINGS PROFILE prof1")
    instance.query("DROP ROW POLICY rowpol1 ON test.table")
    instance.query("DROP QUOTA q1")

    instance.query(
        f"RESTORE TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas FROM {backup_name}"
    )

    assert (
        instance.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH sha256_password SETTINGS PROFILE `default`, custom_a = 1\n"
    )
    assert instance.query("SHOW GRANTS FOR u1") == TSV(
        ["GRANT SELECT ON test.* TO u1", "GRANT r2 TO u1"]
    )
    assert instance.query("SHOW CREATE ROLE r1") == "CREATE ROLE r1\n"
    assert instance.query("SHOW GRANTS FOR r1") == ""
    assert instance.query("SHOW CREATE ROLE r2") == "CREATE ROLE r2\n"
    assert instance.query("SHOW GRANTS FOR r2") == TSV(
        ["GRANT r1 TO r2 WITH ADMIN OPTION"]
    )

    assert (
        instance.query("SHOW CREATE SETTINGS PROFILE prof1")
        == "CREATE SETTINGS PROFILE `prof1` SETTINGS custom_b = 2 TO u1\n"
    )
    assert (
        instance.query("SHOW CREATE ROW POLICY rowpol1")
        == "CREATE ROW POLICY rowpol1 ON test.`table` FOR SELECT USING x < 50 TO u1\n"
    )
    assert instance.query("SHOW CREATE QUOTA q1") == "CREATE QUOTA q1 TO r1\n"


def test_system_users_required_privileges():
    instance.query("CREATE ROLE r1")
    instance.query("CREATE USER u1 DEFAULT ROLE r1")
    instance.query("GRANT SELECT ON test.* TO u1")

    # SETTINGS allow_backup=false means the following user won't be included in backups.
    instance.query("CREATE USER u2 SETTINGS allow_backup=false")

    backup_name = new_backup_name()

    expected_error = "necessary to have the grant BACKUP ON system.users"
    assert expected_error in instance.query_and_get_error(
        f"BACKUP TABLE system.users, TABLE system.roles TO {backup_name}", user="u2"
    )

    instance.query("GRANT BACKUP ON system.users TO u2")

    expected_error = "necessary to have the grant BACKUP ON system.roles"
    assert expected_error in instance.query_and_get_error(
        f"BACKUP TABLE system.users, TABLE system.roles TO {backup_name}", user="u2"
    )

    instance.query("GRANT BACKUP ON system.roles TO u2")
    instance.query(
        f"BACKUP TABLE system.users, TABLE system.roles TO {backup_name}", user="u2"
    )

    instance.query("DROP USER u1")
    instance.query("DROP ROLE r1")

    expected_error = "necessary to have the grant ROLE ADMIN ON *.*"
    assert expected_error in instance.query_and_get_error(
        f"RESTORE ALL FROM {backup_name}", user="u2"
    )

    instance.query("GRANT ROLE ADMIN ON *.* TO u2")

    expected_error = "necessary to have the grant CREATE ROLE ON r1"
    assert expected_error in instance.query_and_get_error(
        f"RESTORE ALL FROM {backup_name}", user="u2"
    )

    instance.query("GRANT CREATE USER, CREATE ROLE, ROLE ADMIN ON *.* TO u2")

    expected_error = "necessary to have the grant SELECT ON test.* WITH GRANT OPTION"
    assert expected_error in instance.query_and_get_error(
        f"RESTORE ALL FROM {backup_name}", user="u2"
    )

    instance.query("GRANT SELECT ON test.* TO u2 WITH GRANT OPTION")
    instance.query(f"RESTORE ALL FROM {backup_name}", user="u2")

    assert (
        instance.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH no_password DEFAULT ROLE r1\n"
    )
    assert instance.query("SHOW GRANTS FOR u1") == TSV(
        ["GRANT SELECT ON test.* TO u1", "GRANT r1 TO u1"]
    )

    assert instance.query("SHOW CREATE ROLE r1") == "CREATE ROLE r1\n"
    assert instance.query("SHOW GRANTS FOR r1") == ""

    instance.query("DROP USER u1")
    instance.query("DROP ROLE r1")
    instance.query("DROP USER u2")


def test_system_users_async():
    instance.query("CREATE USER u1 IDENTIFIED BY 'qwe123' SETTINGS custom_c = 3")

    backup_name = new_backup_name()
    id = instance.query(
        f"BACKUP DATABASE default, TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas TO {backup_name} ASYNC"
    ).split("\t")[0]

    assert_eq_with_retry(
        instance,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["BACKUP_CREATED", ""]]),
    )

    instance.query("DROP USER u1")

    id = instance.query(
        f"RESTORE DATABASE default, TABLE system.users, TABLE system.roles, TABLE system.settings_profiles, TABLE system.row_policies, TABLE system.quotas FROM {backup_name} ASYNC"
    ).split("\t")[0]

    assert_eq_with_retry(
        instance,
        f"SELECT status, error FROM system.backups WHERE id='{id}'",
        TSV([["RESTORED", ""]]),
    )

    assert (
        instance.query("SHOW CREATE USER u1")
        == "CREATE USER u1 IDENTIFIED WITH sha256_password SETTINGS custom_c = 3\n"
    )


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


def test_restore_table_not_evaluate_table_defaults():
    instance.query("CREATE DATABASE test")
    instance.query(
        "CREATE TABLE test.src(key Int64, value Int64) ENGINE=MergeTree ORDER BY key"
    )
    instance.query(
        "INSERT INTO test.src SELECT number as key, number * number AS value FROM numbers(1, 3)"
    )
    instance.query(
        "INSERT INTO test.src SELECT number as key, number * number AS value FROM numbers(6, 3)"
    )
    instance.query("CREATE USER u1")
    instance.query("GRANT SELECT ON test.src TO u1")
    instance.query(
        "CREATE DICTIONARY test.dict(key Int64, value Int64 DEFAULT -1) PRIMARY KEY key SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'test' TABLE 'src' USER u1)) LIFETIME(0) LAYOUT(FLAT())"
    )
    instance.query(
        "CREATE TABLE test.tbl(a Int64, b Int64 DEFAULT 0, c Int64 DEFAULT dictGet(test.dict, 'value', b)) ENGINE=MergeTree ORDER BY a"
    )
    instance.query(
        "INSERT INTO test.tbl (a, b) SELECT number, number + 1 FROM numbers(5)"
    )

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE system.users, DATABASE test TO {backup_name}")

    instance.query("DROP USER u1")

    instance.query(
        f"RESTORE TABLE system.users, DATABASE test AS test2 FROM {backup_name}"
    )

    # RESTORE should not try to load dictionary `test2.dict`
    assert instance.query("SELECT * FROM test2.tbl ORDER BY a") == TSV(
        [[0, 1, 1], [1, 2, 4], [2, 3, 9], [3, 4, -1], [4, 5, -1]]
    )

    assert (
        instance.query(
            "SELECT status FROM system.dictionaries WHERE name = 'dict' AND database = 'test2'"
        )
        == "NOT_LOADED\n"
    )

    # INSERT needs dictionary `test2.dict` and it will cause loading it.
    error = "necessary to have the grant SELECT(key, value) ON test2.src"  # User `u1` has no privileges for reading `test2.src`
    assert error in instance.query_and_get_error(
        "INSERT INTO test2.tbl (a, b) SELECT number, number + 1 FROM numbers(5, 5)"
    )

    assert (
        instance.query(
            "SELECT status FROM system.dictionaries WHERE name = 'dict' AND database = 'test2'"
        )
        == "FAILED\n"
    )

    instance.query("GRANT SELECT ON test2.src TO u1")
    instance.query("SYSTEM RELOAD DICTIONARY test2.dict")

    assert (
        instance.query(
            "SELECT status FROM system.dictionaries WHERE name = 'dict' AND database = 'test2'"
        )
        == "LOADED\n"
    )

    instance.query(
        "INSERT INTO test2.tbl (a, b) SELECT number, number + 1 FROM numbers(5, 5)"
    )

    assert instance.query("SELECT * FROM test2.tbl ORDER BY a") == TSV(
        [
            [0, 1, 1],
            [1, 2, 4],
            [2, 3, 9],
            [3, 4, -1],
            [4, 5, -1],
            [5, 6, 36],
            [6, 7, 49],
            [7, 8, 64],
            [8, 9, -1],
            [9, 10, -1],
        ]
    )


def test_system_functions():
    instance.query("CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;")

    instance.query("CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');")

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE system.functions TO {backup_name}")

    instance.query("DROP FUNCTION linear_equation")
    instance.query("DROP FUNCTION parity_str")

    instance.query(f"RESTORE TABLE system.functions FROM {backup_name}")

    assert instance.query(
        "SELECT number, linear_equation(number, 2, 1) FROM numbers(3)"
    ) == TSV([[0, 1], [1, 3], [2, 5]])

    assert instance.query("SELECT number, parity_str(number) FROM numbers(3)") == TSV(
        [[0, "even"], [1, "odd"], [2, "even"]]
    )
    instance.query("DROP FUNCTION linear_equation")
    instance.query("DROP FUNCTION parity_str")


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
            "query_metric_log",
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
            "error_log",
            "blob_storage_log",
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
    restore_command = f"RESTORE ALL FROM {backup_name} {'SETTINGS ' + ', '.join(restore_settings) if restore_settings else ''}"

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


@pytest.mark.parametrize("include_database_name", [False, True])
def test_backup_database_except(include_database_name):
    create_and_fill_table()

    session_id = new_session_id()
    instance.query(
        "CREATE TABLE test.omit_table (s String) ENGINE = MergeTree ORDER BY s",
    )

    omit_table_name = "test.omit_table" if include_database_name else "omit_table"
    backup_name = new_backup_name()
    backup_command = (
        f"BACKUP DATABASE test EXCEPT TABLES {omit_table_name} TO {backup_name}"
    )

    instance.http_query(backup_command, params={"session_id": session_id})

    instance.query("DROP TABLE test.table")
    instance.query("DROP TABLE test.omit_table")

    restore_command = f"RESTORE ALL FROM {backup_name}"

    session_id = new_session_id()
    instance.http_query(
        restore_command, params={"session_id": session_id}, method="POST"
    )

    assert instance.query("SELECT count(), sum(x) FROM test.table") == "100\t4950\n"
    assert instance.query("EXISTS TABLE test.omit_table") == "0\n"

    instance.query("DROP TABLE test.table")


def test_operation_id():
    create_and_fill_table(n=30)

    backup_name = new_backup_name()

    [id, status] = instance.query(
        f"BACKUP TABLE test.table TO {backup_name} SETTINGS id='first' ASYNC"
    ).split("\t")

    assert id == "first"
    assert status == "CREATING_BACKUP\n" or status == "BACKUP_CREATED\n"

    assert_eq_with_retry(
        instance,
        f"SELECT status, error FROM system.backups WHERE id='first'",
        TSV([["BACKUP_CREATED", ""]]),
    )

    instance.query("DROP TABLE test.table")

    [id, status] = instance.query(
        f"RESTORE TABLE test.table FROM {backup_name} SETTINGS id='second' ASYNC"
    ).split("\t")

    assert id == "second"
    assert status == "RESTORING\n" or status == "RESTORED\n"

    assert_eq_with_retry(
        instance,
        f"SELECT status, error FROM system.backups WHERE id='second'",
        TSV([["RESTORED", ""]]),
    )

    # Reuse the same ID again
    instance.query("DROP TABLE test.table")

    [id, status] = instance.query(
        f"RESTORE TABLE test.table FROM {backup_name} SETTINGS id='first'"
    ).split("\t")

    assert id == "first"
    assert status == "RESTORED\n"


def test_system_backups():
    # Backup
    create_and_fill_table(n=30)

    backup_name = new_backup_name()
    id = instance.query(f"BACKUP TABLE test.table TO {backup_name}").split("\t")[0]

    info = get_backup_info_from_system_backups(by_id=id)
    escaped_backup_name = backup_name.replace("'", "\\'")
    assert info.name == escaped_backup_name
    assert info.status == "BACKUP_CREATED"
    assert info.error == ""
    assert info.num_files > 0
    assert info.total_size > 0
    assert info.start_time < info.end_time
    assert 0 < info.num_entries and info.num_entries <= info.num_files
    assert info.uncompressed_size > 0
    assert info.compressed_size == info.uncompressed_size
    assert info.files_read == 0
    assert info.bytes_read == 0

    files_in_backup_folder = find_files_in_backup_folder(backup_name)
    assert info.num_entries == len(files_in_backup_folder) - 1
    assert info.uncompressed_size == sum(
        os.path.getsize(f) for f in files_in_backup_folder
    )

    # The concrete values can change.
    info.num_files == 91
    info.total_size == 4973
    info.num_entries == 55
    info.uncompressed_size == 19701

    instance.query("DROP TABLE test.table")

    # Restore
    restore_query_id = uuid.uuid4().hex
    id = instance.query(
        f"RESTORE TABLE test.table FROM {backup_name}", query_id=restore_query_id
    ).split("\t")[0]

    def verify_restore_info():
        restore_info = get_backup_info_from_system_backups(by_id=id)
        restore_events = get_events_for_query(restore_query_id)
        return (
            restore_info.name == escaped_backup_name and
            restore_info.status == "RESTORED" and
            restore_info.error == "" and
            restore_info.start_time < restore_info.end_time and
            restore_info.num_files == info.num_files and
            restore_info.total_size == info.total_size and
            restore_info.num_entries == info.num_entries and
            restore_info.uncompressed_size == info.uncompressed_size and
            restore_info.compressed_size == info.compressed_size and
            restore_info.files_read + restore_events["RestorePartsSkippedFiles"] == restore_info.num_files and
            restore_info.bytes_read + restore_events["RestorePartsSkippedBytes"] == restore_info.total_size
        )

    wait_condition(verify_restore_info, lambda x: x)

    # Failed backup.
    backup_name = new_backup_name()
    expected_error = "Table test.non_existent_table was not found"
    assert expected_error in instance.query_and_get_error(
        f"BACKUP TABLE test.non_existent_table TO {backup_name}"
    )

    escaped_backup_name = backup_name.replace("'", "\\'")
    info = get_backup_info_from_system_backups(by_name=escaped_backup_name)

    assert info.status == "BACKUP_FAILED"
    assert expected_error in info.error
    assert info.num_files == 0
    assert info.total_size == 0
    assert info.num_entries == 0
    assert info.uncompressed_size == 0
    assert info.compressed_size == 0
    assert info.files_read == 0
    assert info.bytes_read == 0


def test_mutation():
    create_and_fill_table(engine="MergeTree ORDER BY tuple()", n=5)

    instance.query(
        "INSERT INTO test.table SELECT number, toString(number) FROM numbers(5, 5)"
    )

    instance.query(
        "INSERT INTO test.table SELECT number, toString(number) FROM numbers(10, 5)"
    )

    instance.query("ALTER TABLE test.table UPDATE x=x+1 WHERE 1")
    instance.query("ALTER TABLE test.table UPDATE x=x+1+sleep(3) WHERE 1")
    instance.query("ALTER TABLE test.table UPDATE x=x+1+sleep(3) WHERE 1")

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.table TO {backup_name}")

    assert not has_mutation_in_backup("0000000004", backup_name, "test", "table")
    assert has_mutation_in_backup("0000000005", backup_name, "test", "table")
    assert has_mutation_in_backup("0000000006", backup_name, "test", "table")
    assert not has_mutation_in_backup("0000000007", backup_name, "test", "table")

    instance.query("DROP TABLE test.table")

    instance.query(f"RESTORE TABLE test.table FROM {backup_name}")


def test_tables_dependency():
    instance.query("CREATE DATABASE test")
    instance.query("CREATE DATABASE test2")

    # For this test we use random names of tables to check they're created according to their dependency (not just in alphabetic order).
    random_table_names = [f"{chr(ord('A')+i)}" for i in range(0, 15)]
    random.shuffle(random_table_names)
    random_table_names = [
        random.choice(["test", "test2"]) + "." + table_name
        for table_name in random_table_names
    ]
    print(f"random_table_names={random_table_names}")
    t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15 = tuple(
        random_table_names
    )

    # Create a materialized view and a dictionary with a local table as source.
    instance.query(
        f"CREATE TABLE {t1} (x Int64, y String) ENGINE=MergeTree ORDER BY tuple()"
    )

    instance.query(
        f"CREATE TABLE {t2} (x Int64, y String) ENGINE=MergeTree ORDER BY tuple()"
    )

    instance.query(f"CREATE MATERIALIZED VIEW {t3} TO {t2} AS SELECT x, y FROM {t1}")

    instance.query(
        f"CREATE DICTIONARY {t4} (x Int64, y String) PRIMARY KEY x SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE '{t1.split('.')[1]}' DB '{t1.split('.')[0]}')) LAYOUT(FLAT()) LIFETIME(4)"
    )

    instance.query(f"CREATE TABLE {t5} AS dictionary({t4})")

    instance.query(
        f"CREATE TABLE {t6}(x Int64, y String DEFAULT dictGet({t4}, 'y', x)) ENGINE=MergeTree ORDER BY tuple()"
    )

    instance.query(f"CREATE VIEW {t7} AS SELECT sum(x) FROM (SELECT x FROM {t6})")

    instance.query(
        f"CREATE DICTIONARY {t8} (x Int64, y String) PRIMARY KEY x SOURCE(CLICKHOUSE(TABLE '{t1.split('.')[1]}' DB '{t1.split('.')[0]}')) LAYOUT(FLAT()) LIFETIME(9)"
    )

    instance.query(f"CREATE TABLE {t9}(a Int64) ENGINE=Log")

    instance.query(
        f"CREATE VIEW {t10}(x Int64, y String) AS SELECT * FROM {t1} WHERE x IN {t9}"
    )

    instance.query(
        f"CREATE VIEW {t11}(x Int64, y String) AS SELECT * FROM {t2} WHERE x NOT IN (SELECT a FROM {t9})"
    )

    instance.query(
        f"CREATE TABLE {t12} AS {t1} ENGINE = Buffer({t2.split('.')[0]}, {t2.split('.')[1]}, 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
    )

    instance.query(
        f"CREATE TABLE {t13} AS {t1} ENGINE = Buffer((SELECT '{t2.split('.')[0]}'), (SELECT '{t2.split('.')[1]}'), 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
    )

    instance.query(
        f"CREATE TABLE {t14} AS {t1} ENGINE = Buffer('', {t2.split('.')[1]}, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
        database=t2.split(".")[0],
    )

    instance.query(
        f"CREATE TABLE {t15} AS {t1} ENGINE = Buffer('', '', 16, 10, 100, 10000, 1000000, 10000000, 100000000)"
    )

    # Make backup.
    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE test, DATABASE test2 TO {backup_name}")

    # Drop everything in reversive order.
    def drop():
        instance.query(f"DROP TABLE {t15} SYNC")
        instance.query(f"DROP TABLE {t14} SYNC")
        instance.query(f"DROP TABLE {t13} SYNC")
        instance.query(f"DROP TABLE {t12} SYNC")
        instance.query(f"DROP TABLE {t11} SYNC")
        instance.query(f"DROP TABLE {t10} SYNC")
        instance.query(f"DROP TABLE {t9} SYNC")
        instance.query(f"DROP DICTIONARY {t8}")
        instance.query(f"DROP TABLE {t7} SYNC")
        instance.query(f"DROP TABLE {t6} SYNC")
        instance.query(f"DROP TABLE {t5} SYNC")
        instance.query(f"DROP DICTIONARY {t4}")
        instance.query(f"DROP TABLE {t3} SYNC")
        instance.query(f"DROP TABLE {t2} SYNC")
        instance.query(f"DROP TABLE {t1} SYNC")
        instance.query("DROP DATABASE test SYNC")
        instance.query("DROP DATABASE test2 SYNC")

    drop()

    # Restore everything.
    instance.query(f"RESTORE ALL FROM {backup_name}")

    # Check everything is restored.
    assert instance.query(
        "SELECT concat(database, '.', name) AS c FROM system.tables WHERE database IN ['test', 'test2'] ORDER BY c"
    ) == TSV(sorted(random_table_names))

    # Check logs.
    instance.query("SYSTEM FLUSH LOGS")
    expect_in_logs = [
        f"Table {t1} has no dependencies (level 0)",
        f"Table {t2} has no dependencies (level 0)",
        (
            f"Table {t3} has 2 dependencies: {t1}, {t2} (level 1)",
            f"Table {t3} has 2 dependencies: {t2}, {t1} (level 1)",
        ),
        f"Table {t4} has 1 dependencies: {t1} (level 1)",
        f"Table {t5} has 1 dependencies: {t4} (level 2)",
        f"Table {t6} has 1 dependencies: {t4} (level 2)",
        f"Table {t7} has 1 dependencies: {t6} (level 3)",
        f"Table {t8} has 1 dependencies: {t1} (level 1)",
        f"Table {t9} has no dependencies (level 0)",
        (
            f"Table {t10} has 2 dependencies: {t1}, {t9} (level 1)",
            f"Table {t10} has 2 dependencies: {t9}, {t1} (level 1)",
        ),
        (
            f"Table {t11} has 2 dependencies: {t2}, {t9} (level 1)",
            f"Table {t11} has 2 dependencies: {t9}, {t2} (level 1)",
        ),
        f"Table {t12} has 1 dependencies: {t2} (level 1)",
        f"Table {t13} has 1 dependencies: {t2} (level 1)",
        f"Table {t14} has 1 dependencies: {t2} (level 1)",
        f"Table {t15} has no dependencies (level 0)",
    ]
    for expect in expect_in_logs:
        assert any(
            [
                instance.contains_in_log(f"RestorerFromBackup: {x}")
                for x in tuple(expect)
            ]
        )

    drop()


def test_required_privileges_with_partial_revokes():
    backup_name = new_backup_name()
    instance.query("CREATE USER u1")
    instance.query("GRANT SELECT ON *.* TO u1")
    instance.query("REVOKE SELECT ON system.zookeeper* FROM u1")
    instance.query("REVOKE SELECT ON foo.* FROM u1")

    instance.query(f"BACKUP TABLE system.users TO {backup_name}")
    instance.query("DROP USER u1")

    instance.query("CREATE USER u2")
    instance.query("GRANT SELECT ON *.* TO u2 WITH GRANT OPTION")
    instance.query("GRANT CREATE USER ON *.* TO u2")
    instance.query("REVOKE SELECT ON system.zookeeper* FROM u2")
    instance.query("REVOKE SELECT ON foo.* FROM u2")

    instance.query(f"RESTORE ALL FROM {backup_name}", user="u2")
    instance.query("DROP USER u1")

    instance.query("REVOKE SELECT ON f* FROM u2")
    # To restore the backup we should have the SELECT permission on any table except system.zookeeper* and foo.*, but now we don't have it on f*.
    assert "Not enough privileges" in instance.query_and_get_error(
        f"RESTORE ALL FROM {backup_name}", user="u2"
    )


# Test for the "clickhouse_backupview" utility.

test_backupview_dir = os.path.abspath(
    os.path.join(script_dir, "../../../utils/backupview/test")
)
if test_backupview_dir not in sys.path:
    sys.path.append(test_backupview_dir)
import test_backupview as test_backupview_module


def test_backupview():
    if instance.is_built_with_sanitizer():
        return  # This test is actually for clickhouse_backupview, not for ClickHouse itself.
    test_backupview_module.test_backupview_1()
