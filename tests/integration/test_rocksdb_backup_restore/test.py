# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

# BACKUP/RESTORE of EmbeddedRocksDB tables that live on an explicit `rocksdb_dir`, including the
# supported shared-directory modes (one writable table plus read_only siblings, and groups of
# read_only tables only). See https://github.com/ClickHouse/ClickHouse/issues/109213
#
# Every case here has to create and wipe a RocksDB directory on the server's filesystem, which a
# stateless test is not allowed to do, hence an integration test.

import threading

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/backups_disk.xml"],
    external_dirs=["/backups/"],
)

# A relative `rocksdb_dir` is resolved under user_files_path, so that is where the directories these
# tests lay down end up.
USER_FILES_PATH = "/var/lib/clickhouse/user_files"

# Error fragments that tell the CANNOT_RESTORE_TABLE rejections apart. Asserting only the error code
# would let one guard's failure be reported as another guard's success.
READ_ONLY_REJECTION = "Cannot restore data into read_only EmbeddedRocksDB table"
NOT_EMPTY_REJECTION = "already contains some data"
TTL_REJECTION = "target table has ttl"
SCHEMA_REJECTION = "column layout does not match the target"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def backup_to(name):
    return f"Disk('backups', '{name}')"


def reset(case):
    """Give `case` an empty database, no leftover RocksDB directories and no leftover backups."""
    node.query(f"DROP DATABASE IF EXISTS {case} SYNC")
    node.exec_in_container(
        ["bash", "-c", f"rm -rf {USER_FILES_PATH}/{case}_* /backups/{case}*"]
    )
    node.query(f"CREATE DATABASE {case}")


def restore_error(query):
    """Run a RESTORE that must fail, and return the error text for the caller to discriminate on."""
    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(query)
    error = str(exc_info.value)
    assert "CANNOT_RESTORE_TABLE" in error, error
    return error


def test_restore_of_non_empty_read_only_backup_is_rejected():
    # A read_only handle is opened with OpenForReadOnly() / DBWithTTL::Open(..., read_only) and rejects
    # writes, so replaying rows into it cannot work. The restore must say so up front instead of
    # failing later with an opaque RocksDB write error.
    case = "rdb_ro_nonempty"
    reset(case)
    directory = f"{case}_dir"
    # A read_only table can only open a directory that already exists, so populate it through a
    # writable table first and drop that table again (the directory stays).
    node.query(f"""
        CREATE TABLE {case}.rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}') PRIMARY KEY k;
        INSERT INTO {case}.rw SELECT number, 'v' || toString(number) FROM numbers(100);
        DROP TABLE {case}.rw SYNC;
        CREATE TABLE {case}.ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}', 1) PRIMARY KEY k;
        BACKUP TABLE {case}.ro TO {backup_to(case)} FORMAT Null;
        DROP TABLE {case}.ro SYNC;
        """)
    error = restore_error(f"RESTORE TABLE {case}.ro FROM {backup_to(case)} FORMAT Null")
    assert READ_ONLY_REJECTION in error, error


def test_empty_read_only_backup_restores():
    # The backup of an empty read_only table carries no rows, so its restore needs no write: it must
    # succeed as a pure metadata restore rather than hit the read_only rejection.
    case = "rdb_ro_empty"
    reset(case)
    directory = f"{case}_dir"
    node.query(f"""
        CREATE TABLE {case}.rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}') PRIMARY KEY k;
        DROP TABLE {case}.rw SYNC;
        CREATE TABLE {case}.ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}', 1) PRIMARY KEY k;
        BACKUP TABLE {case}.ro TO {backup_to(case)} FORMAT Null;
        DROP TABLE {case}.ro SYNC;
        """)
    node.query(f"RESTORE TABLE {case}.ro FROM {backup_to(case)} FORMAT Null")
    assert node.query(f"SELECT count() FROM {case}.ro").strip() == "0"


def test_empty_backup_over_populated_read_only_directory():
    # A read_only table always points at an externally managed directory, so its rows can change
    # behind the backup's back. Restoring an empty backup over a directory that now holds rows must
    # hit the non-empty-table guard instead of silently "succeeding"; with allow_non_empty_tables it
    # is allowed, writes nothing, and the rows stay.
    case = "rdb_ro_populated"
    reset(case)
    directory = f"{case}_dir"
    node.query(f"""
        CREATE TABLE {case}.rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}') PRIMARY KEY k;
        DROP TABLE {case}.rw SYNC;
        CREATE TABLE {case}.ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}', 1) PRIMARY KEY k;
        BACKUP TABLE {case}.ro TO {backup_to(case)} FORMAT Null;
        DROP TABLE {case}.ro SYNC;
        CREATE TABLE {case}.rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}') PRIMARY KEY k;
        INSERT INTO {case}.rw SELECT number, 'stale' || toString(number) FROM numbers(100);
        DROP TABLE {case}.rw SYNC;
        """)
    error = restore_error(f"RESTORE TABLE {case}.ro FROM {backup_to(case)} FORMAT Null")
    assert NOT_EMPTY_REJECTION in error, error
    node.query(
        f"RESTORE TABLE {case}.ro FROM {backup_to(case)} SETTINGS allow_non_empty_tables = 1 FORMAT Null"
    )
    assert node.query(f"SELECT count() FROM {case}.ro").strip() == "100"


def test_shared_directory_with_writable_and_read_only_table():
    # The supported shared-directory mode: one writable table plus a read_only table on the same
    # `rocksdb_dir` (see test_rocksdb_read_only). BACKUP DATABASE must dump the shared RocksDB once,
    # from the writable owner, with the read_only sibling referencing that single dump; on restore the
    # owner replays the rows once and the sibling contributes no write, so the pair restores cleanly.
    #
    # The read_only handle is opened while the directory is still empty, so its snapshot holds 0 rows
    # and stays at 0 across the writable table's inserts (a read_only handle does not see writes made
    # through another handle). That makes the post-restore read a real discriminator: the sibling can
    # only report the restored 300 rows if finalizeRestoreFromBackup() refreshed its handle.
    case = "rdb_shared"
    reset(case)
    directory = f"{case}_dir"
    node.query(f"""
        CREATE TABLE {case}.rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}') PRIMARY KEY k;
        CREATE TABLE {case}.ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}', 1) PRIMARY KEY k;
        INSERT INTO {case}.rw SELECT number, 'v' || toString(number) FROM numbers(300);
        """)
    assert node.query(f"SELECT count() FROM {case}.ro").strip() == "0"
    node.query(f"BACKUP DATABASE {case} TO {backup_to(case)} FORMAT Null")
    # Restored in place, so the writable table already holds the rows and allow_non_empty_tables is
    # required. The point is that the restore does not fail on the read_only sibling.
    node.query(
        f"RESTORE DATABASE {case} FROM {backup_to(case)} SETTINGS allow_non_empty_tables = 1 FORMAT Null"
    )
    assert node.query(f"SELECT count(), sum(k) FROM {case}.rw").strip() == "300\t44850"
    assert node.query(f"SELECT count(), sum(k) FROM {case}.ro").strip() == "300\t44850"
    # The restore above passes whether or not the backup deduplicated, because the read_only sibling's
    # restore task is skipped either way and its handle is then reopened over the writable table's
    # directory. Restoring the read_only table on its own into a fresh writable table is what pins the
    # backup side down: its data entry references the owner's dump, so it yields the owner's 300 rows,
    # where an independent read_only dump would yield the 0 rows of its stale snapshot.
    node.query(
        f"CREATE TABLE {case}.ro_alone (k UInt64, v String) "
        f"ENGINE = EmbeddedRocksDB(0, '{case}_ro_alone') PRIMARY KEY k"
    )
    node.query(
        f"RESTORE TABLE {case}.ro AS {case}.ro_alone FROM {backup_to(case)} "
        f"SETTINGS allow_different_table_def = 1 FORMAT Null"
    )
    assert (
        node.query(f"SELECT count(), sum(k) FROM {case}.ro_alone").strip()
        == "300\t44850"
    )


def test_all_read_only_group_keeps_independent_snapshots():
    # A group of read_only tables on one directory with no writable sibling. The single-owner dedup
    # must not apply: read_only handles are independent snapshots that can diverge, so there is no
    # single live view representing every table, and collapsing them onto one owner would make the
    # other backup reference the wrong data.
    #
    # The two handles are given genuinely different snapshots (100 rows and 200 rows), so restoring
    # each backup as a fresh writable table proves each recovered its own.
    case = "rdb_all_read_only"
    reset(case)
    directory = f"{case}_dir"
    node.query(f"""
        CREATE TABLE {case}.feeder (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}') PRIMARY KEY k;
        INSERT INTO {case}.feeder SELECT number, 'v' || toString(number) FROM numbers(100);
        DROP TABLE {case}.feeder SYNC;
        CREATE TABLE {case}.ro_a (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}', 1) PRIMARY KEY k;
        CREATE TABLE {case}.feeder (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}') PRIMARY KEY k;
        INSERT INTO {case}.feeder SELECT number, 'v' || toString(number) FROM numbers(100, 100);
        DROP TABLE {case}.feeder SYNC;
        CREATE TABLE {case}.ro_b (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}', 1) PRIMARY KEY k;
        """)
    assert node.query(f"SELECT count() FROM {case}.ro_a").strip() == "100"
    assert node.query(f"SELECT count() FROM {case}.ro_b").strip() == "200"
    node.query(f"BACKUP DATABASE {case} TO {backup_to(case)} FORMAT Null")
    for source, expected in [("ro_a", "100"), ("ro_b", "200")]:
        target = f"restored_{source}"
        node.query(
            f"CREATE TABLE {case}.{target} (k UInt64, v String) "
            f"ENGINE = EmbeddedRocksDB(0, '{case}_{target}') PRIMARY KEY k"
        )
        node.query(
            f"RESTORE TABLE {case}.{source} AS {case}.{target} FROM {backup_to(case)} "
            f"SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1 FORMAT Null"
        )
        assert node.query(f"SELECT count() FROM {case}.{target}").strip() == expected


def test_ttl_mismatch_is_rejected():
    # The backed-up value bytes are ttl-format dependent: a ttl > 0 table is a DBWithTTL whose values
    # carry a trailing creation timestamp, a ttl = 0 table's do not. Restoring across a ttl mismatch
    # would replay incompatible bytes or shift every row's expiry. RESTORE ... AS with
    # allow_different_table_def skips the create-query compatibility check, so restore checks the ttl
    # itself. Both directions must be rejected, and a matching ttl must still work.
    case = "rdb_ttl"
    reset(case)
    node.query(f"""
        CREATE TABLE {case}.src_ttl0 (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{case}_src_ttl0') PRIMARY KEY k;
        INSERT INTO {case}.src_ttl0 SELECT number, 'v' || toString(number) FROM numbers(50);
        BACKUP TABLE {case}.src_ttl0 TO {backup_to(case)} FORMAT Null;
        CREATE TABLE {case}.src_ttl5 (k UInt64, v String) ENGINE = EmbeddedRocksDB(5, '{case}_src_ttl5') PRIMARY KEY k;
        INSERT INTO {case}.src_ttl5 SELECT number, 'v' || toString(number) FROM numbers(50);
        BACKUP TABLE {case}.src_ttl5 TO {backup_to(case + "_5")} FORMAT Null;
        CREATE TABLE {case}.dst_ttl0 (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{case}_dst_ttl0') PRIMARY KEY k;
        CREATE TABLE {case}.dst_ttl5 (k UInt64, v String) ENGINE = EmbeddedRocksDB(5, '{case}_dst_ttl5') PRIMARY KEY k;
        """)
    settings = "SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1"
    error = restore_error(
        f"RESTORE TABLE {case}.src_ttl0 AS {case}.dst_ttl5 FROM {backup_to(case)} {settings} FORMAT Null"
    )
    assert TTL_REJECTION in error, error
    error = restore_error(
        f"RESTORE TABLE {case}.src_ttl5 AS {case}.dst_ttl0 FROM {backup_to(case + '_5')} {settings} FORMAT Null"
    )
    assert TTL_REJECTION in error, error
    node.query(
        f"RESTORE TABLE {case}.src_ttl0 AS {case}.dst_ttl0 FROM {backup_to(case)} {settings} FORMAT Null"
    )
    assert node.query(f"SELECT count() FROM {case}.dst_ttl0").strip() == "50"


def test_schema_mismatch_is_rejected():
    # Restore replays raw (key, value) bytes and they are decoded with the target table's schema: key
    # = primary-key columns in primary-key order, value = the remaining physical columns in physical
    # order. A target whose column types or ordering differ would silently decode the bytes into
    # wrong data, and RESTORE ... AS with allow_different_table_def skips the create-query
    # compatibility check, so restore compares a schema fingerprint itself.
    case = "rdb_schema"
    reset(case)
    node.query(f"""
        CREATE TABLE {case}.src (k UInt64, a String, b UInt64) ENGINE = EmbeddedRocksDB(0, '{case}_src') PRIMARY KEY k;
        INSERT INTO {case}.src SELECT number, 'v' || toString(number), number * 2 FROM numbers(50);
        BACKUP TABLE {case}.src TO {backup_to(case)} FORMAT Null;
        CREATE TABLE {case}.dst_type (k UInt64, a UInt64, b UInt64) ENGINE = EmbeddedRocksDB(0, '{case}_dst_type') PRIMARY KEY k;
        CREATE TABLE {case}.dst_order (k UInt64, b UInt64, a String) ENGINE = EmbeddedRocksDB(0, '{case}_dst_order') PRIMARY KEY k;
        CREATE TABLE {case}.dst_same (k UInt64, a String, b UInt64) ENGINE = EmbeddedRocksDB(0, '{case}_dst_same') PRIMARY KEY k;
        """)
    settings = "SETTINGS allow_non_empty_tables = 1, allow_different_table_def = 1"
    for target in ["dst_type", "dst_order"]:
        error = restore_error(
            f"RESTORE TABLE {case}.src AS {case}.{target} FROM {backup_to(case)} {settings} FORMAT Null"
        )
        assert SCHEMA_REJECTION in error, error
    node.query(
        f"RESTORE TABLE {case}.src AS {case}.dst_same FROM {backup_to(case)} {settings} FORMAT Null"
    )
    assert (
        node.query(f"SELECT count(), sum(b) FROM {case}.dst_same").strip() == "50\t2450"
    )


def test_restore_does_not_disturb_a_concurrent_read_only_scan():
    # finalizeRestoreFromBackup() replaces the read_only sibling's RocksDB handle, and
    # RestorerFromBackup::finalizeTables() holds only a shared table lock, so unlike truncate() and
    # drop() it does not exclude readers: a full scan of that sibling can be iterating the handle at
    # that moment. RocksDB aborts if a database is closed while an iterator into it is alive, so the
    # restore has to hand the handle over to the scan rather than close it.
    case = "rdb_concurrent"
    reset(case)
    directory = f"{case}_dir"
    node.query(f"""
        CREATE TABLE {case}.rw (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}') PRIMARY KEY k;
        INSERT INTO {case}.rw SELECT number, 'v' || toString(number) FROM numbers(300);
        """)
    # Opened after the first insert, so this handle's snapshot holds those rows and scanning it takes
    # long enough to still be in flight when the restore finalizes.
    node.query(
        f"CREATE TABLE {case}.ro (k UInt64, v String) ENGINE = EmbeddedRocksDB(0, '{directory}', 1) PRIMARY KEY k"
    )
    # The rest of the rows land after that, so the read_only snapshot and the directory differ: 300
    # rows means the scan kept its own handle, 500 means it was served the reopened one.
    node.query(
        f"INSERT INTO {case}.rw SELECT number, 'v' || toString(number) FROM numbers(300, 200)"
    )
    assert node.query(f"SELECT count() FROM {case}.ro").strip() == "300"
    node.query(f"BACKUP DATABASE {case} TO {backup_to(case)} FORMAT Null")

    # One row per block with a sleep per row: ~9s of iteration inside the full-scan source.
    scanned = []

    def scan():
        scanned.append(
            node.query(
                f"SELECT count() FROM {case}.ro WHERE NOT ignore(sleepEachRow(0.03)) "
                f"SETTINGS max_block_size = 1",
                query_id=f"{case}_scan",
            ).strip()
        )

    scanner = threading.Thread(target=scan)
    scanner.start()
    try:
        # Barrier rather than a sleep. It waits for rows already read, not just for the query to be
        # in the process list: the list entry is made before the pipeline is built, so waiting on it
        # alone would let the restore finish before the iterator this test is about even exists.
        assert_eq_with_retry(
            node,
            f"SELECT read_rows > 0 FROM system.processes WHERE query_id = '{case}_scan'",
            "1",
        )
        node.query(
            f"RESTORE DATABASE {case} FROM {backup_to(case)} SETTINGS allow_non_empty_tables = 1 FORMAT Null"
        )
    finally:
        scanner.join()
    # The in-flight scan finishes on the snapshot it started from rather than on the handle the
    # restore installed, and the server is still there afterwards to serve that new handle. Without
    # the fix the restore aborts the server inside RocksDB, which fails both of these.
    assert scanned == ["300"]
    assert node.query(f"SELECT count(), sum(k) FROM {case}.ro").strip() == "500\t124750"
