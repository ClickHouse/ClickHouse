import concurrent.futures

import pytest

from helpers.cluster import ClickHouseCluster

# These cases belong here rather than in 04648_rocksdb_rename_moves_data.sh because each one
# needs a directory planted inside the server's own table data directory. A stateless test may
# not do that: the suite also runs on object storage and encrypted disks, where the local
# layout does not exist or does not mean what the test assumes.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

ORDINARY_DB = "rocksdb_rename_ord"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def clean_database():
    node.query(
        f"DROP DATABASE IF EXISTS {ORDINARY_DB} SYNC",
        settings={"force_remove_data_recursively_on_drop": 1},
    )
    node.query(
        f"CREATE DATABASE {ORDINARY_DB} ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    yield
    for failpoint in (
        "rocksdb_rename_fail_reopen",
        "rocksdb_rename_pause_before_rollback",
    ):
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
    node.query(
        f"DROP DATABASE IF EXISTS {ORDINARY_DB} SYNC",
        settings={"force_remove_data_recursively_on_drop": 1},
    )


def make_table(table, rows):
    node.query(
        f"""
        CREATE TABLE {ORDINARY_DB}.{table} (k UInt64, v String)
            ENGINE = EmbeddedRocksDB PRIMARY KEY k
        """
    )
    node.query(
        f"INSERT INTO {ORDINARY_DB}.{table} SELECT number, toString(number) FROM numbers({rows})"
    )


def database_data_dir(table):
    """The parent of the table's data directory, so a sibling name can be planted next to it."""
    data_path = node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = '{ORDINARY_DB}' AND name = '{table}'"
    ).strip()
    return data_path.rstrip("/").rsplit("/", 1)[0] + "/"


def reloaded_count(table):
    """DETACH + ATTACH recreates the storage from metadata, the same path a restart takes: the
    data directory is recomputed from the table's current location."""
    node.query(f"DETACH TABLE {ORDINARY_DB}.{table} SYNC")
    node.query(f"ATTACH TABLE {ORDINARY_DB}.{table}")
    return int(node.query(f"SELECT count() FROM {ORDINARY_DB}.{table}").strip())


def plant_directory(path, occupant=None):
    node.exec_in_container(["bash", "-c", f"mkdir -p '{path}'"], user="root")
    if occupant is not None:
        node.exec_in_container(["bash", "-c", f"touch '{path}/{occupant}'"], user="root")


def path_exists(path):
    return (
        node.exec_in_container(
            ["bash", "-c", f"test -e '{path}' && echo 1 || echo 0"], user="root"
        ).strip()
        == "1"
    )


def rename_paused_before_rollback(table, target):
    """Start a RENAME whose reopen fails, and return once it has paused right before its
    rollback. The caller plants a directory at the old location in that window."""
    node.query("SYSTEM ENABLE FAILPOINT rocksdb_rename_fail_reopen")
    node.query("SYSTEM ENABLE FAILPOINT rocksdb_rename_pause_before_rollback")

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    rename_future = pool.submit(
        node.query_and_get_answer_with_error,
        f"RENAME TABLE {ORDINARY_DB}.{table} TO {ORDINARY_DB}.{target}",
    )
    wait_future = pool.submit(
        node.query, "SYSTEM WAIT FAILPOINT rocksdb_rename_pause_before_rollback PAUSE"
    )
    done, _ = concurrent.futures.wait([wait_future], timeout=60)
    if not done:
        pool.shutdown(wait=False, cancel_futures=True)
        raise AssertionError("rocksdb_rename_pause_before_rollback was not reached")
    wait_future.result()
    return pool, rename_future


def resume_and_get_rename_error(pool, rename_future, disable_reopen_failpoint):
    if disable_reopen_failpoint:
        node.query("SYSTEM DISABLE FAILPOINT rocksdb_rename_fail_reopen")
    # DISABLE resumes the paused rollback and removes the failpoint in one step.
    node.query("SYSTEM DISABLE FAILPOINT rocksdb_rename_pause_before_rollback")
    _, error = rename_future.result(timeout=120)
    pool.shutdown(wait=True)
    return error


def test_existing_destination_directory_is_not_replaced(started_cluster):
    """An existing destination directory must not be replaced, and the failed rename must leave
    the table attached and fully readable: the handle is restored and the caller re-attaches it."""
    make_table("t10", 5)
    data_dir = database_data_dir("t10")
    plant_directory(f"{data_dir}t10_target")

    error = node.query_and_get_error(
        f"RENAME TABLE {ORDINARY_DB}.t10 TO {ORDINARY_DB}.t10_target"
    )
    assert "ATOMIC_RENAME_FAIL" in error or "FILE_ALREADY_EXISTS" in error

    assert int(node.query(f"SELECT count() FROM {ORDINARY_DB}.t10").strip()) == 5
    assert reloaded_count("t10") == 5


def test_rollback_removes_empty_directory_at_old_location(started_cluster):
    """The directory was moved, the reopen failed, and by the time the rollback runs the source
    directory exists again (any statement naming it recreates it). The rollback must not decide
    from path existence whether it moved anything, and an empty directory at the old location
    holds no data, so it is removed and the move back retried: the table recovers completely."""
    make_table("t14", 6)
    data_dir = database_data_dir("t14")

    pool, rename_future = rename_paused_before_rollback("t14", "t14_target")
    plant_directory(f"{data_dir}t14")
    # Only the forward reopen had to fail, so the rollback reopens the directory it moved back.
    error = resume_and_get_rename_error(pool, rename_future, True)
    assert "FAULT_INJECTED" in error

    assert int(node.query(f"SELECT count() FROM {ORDINARY_DB}.t14").strip()) == 6
    # The reload is what proves the recovery is on disk and not just in the live object: it
    # recomputes the data directory from the metadata, which names the old location.
    assert reloaded_count("t14") == 6
    assert not path_exists(f"{data_dir}t14_target")


def test_non_empty_directory_at_old_location_is_kept(started_cluster):
    """Same double failure, but the old location holds a directory that is NOT empty. Its content
    was not created by this table, so it must be kept and the data must not be moved back: the
    table stays attached with no usable handle, and reads refuse while naming the directory that
    actually holds the rows instead of reporting zero rows."""
    make_table("t15", 6)
    data_dir = database_data_dir("t15")

    pool, rename_future = rename_paused_before_rollback("t15", "t15_target")
    plant_directory(f"{data_dir}t15", occupant="occupied")
    error = resume_and_get_rename_error(pool, rename_future, False)
    assert "FAULT_INJECTED" in error

    read_error = node.query_and_get_error(f"SELECT * FROM {ORDINARY_DB}.t15")
    assert "ROCKSDB_ERROR" in read_error
    assert f"{data_dir}t15_target" in read_error
    assert "ROCKSDB_ERROR" in node.query_and_get_error(
        f"SELECT * FROM {ORDINARY_DB}.t15 WHERE k = 1"
    )

    assert path_exists(f"{data_dir}t15_target/CURRENT")
    assert path_exists(f"{data_dir}t15/occupied")
