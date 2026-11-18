import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/backups_disk.xml", "configs/keep_backup_files.xml"],
    external_dirs=["/backups/"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def backup_name(name):
    return f"Disk('backups', '{name}')"


def exists(path):
    return (
        node.exec_in_container(
            ["bash", "-c", f"test -e /backups/{path} && echo yes || echo no"]
        ).strip()
        == "yes"
    )


# With `remove_backup_files_after_failure = false` the files of a failed backup are kept, but a backup that
# wrote nothing besides its lock must still release the destination, or every retry is refused.
def test_unwritten_backup_releases_destination():
    prefix = uuid.uuid4().hex
    written = f"{prefix}_written"
    base = f"{prefix}_base"
    manifest = f"{prefix}_manifest"
    incremental = f"{prefix}_incremental"

    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        "CREATE TABLE t (key Int, value String) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query(
        "INSERT INTO t SELECT * FROM generateRandom('key Int, value String') LIMIT 100"
    )

    # Data files were written: they are kept, and so is the lock.
    node.query("SYSTEM ENABLE FAILPOINT backup_fail_before_writing_metadata")
    try:
        assert "FAULT_INJECTED" in node.query_and_get_error(
            f"BACKUP TABLE t TO {backup_name(written)}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT backup_fail_before_writing_metadata")
    assert "BACKUP_ALREADY_EXISTS" in node.query_and_get_error(
        f"BACKUP TABLE t TO {backup_name(written)}"
    )

    assert "BACKUP_CREATED" in node.query(f"BACKUP TABLE t TO {backup_name(base)}")

    # The base has every file, so only the manifest is written, and writing it fails: the lock is kept.
    assert "XML cannot carry unchanged" in node.query_and_get_error(
        f"BACKUP TABLE t TO {backup_name(manifest)} "
        f"SETTINGS base_backup = {backup_name(base)}, id = 'a\\0b {prefix}'"
    )
    assert exists(f"{manifest}/.lock")

    # Nothing was written: the destination is released and the retry succeeds.
    assert "BACKUP_NOT_FOUND" in node.query_and_get_error(
        f"BACKUP TABLE t TO {backup_name(incremental)} "
        f"SETTINGS base_backup = {backup_name(prefix + '_missing')}"
    )
    assert not exists(incremental)
    assert "BACKUP_CREATED" in node.query(
        f"BACKUP TABLE t TO {backup_name(incremental)} "
        f"SETTINGS base_backup = {backup_name(base)}"
    )

    # Nothing was written to a destination that already holds other entries: only the lock is removed.
    prefilled = f"{prefix}_prefilled"
    outside = f"/tmp/{prefix}_outside"
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p /backups/{prefilled}/dir {outside}/dir && ln -s {outside} /backups/{prefilled}/link",
        ]
    )
    assert "BACKUP_NOT_FOUND" in node.query_and_get_error(
        f"BACKUP TABLE t TO {backup_name(prefilled)} "
        f"SETTINGS base_backup = {backup_name(prefix + '_missing')}"
    )
    assert not exists(f"{prefilled}/.lock")
    assert exists(f"{prefilled}/dir")
    assert (
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"test -L /backups/{prefilled}/link && test -d {outside}/dir && echo yes || echo no",
            ]
        ).strip()
        == "yes"
    )
