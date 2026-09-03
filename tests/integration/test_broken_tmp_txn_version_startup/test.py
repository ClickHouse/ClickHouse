import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml", "configs/transactions.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_broken_tmp_txn_version_file_does_not_prevent_startup(started_cluster):
    # A leftover `txn_version.txt.tmp` on a part is an artifact of an interrupted write, so both its
    # content and - on an object storage disk - even the local metadata file describing it can be
    # arbitrary garbage. Loading a part that carries a committed `txn_version.txt` must clean the
    # leftover up (`VersionMetadataOnDisk` documents its content as expendable) instead of failing
    # the table load and with it the server startup: the diagnostic dump of the file in
    # `removeTmpMetadataFile` used to call `getFileSize`, which for an object storage disk
    # deserializes the broken metadata file and throws.
    node.query("DROP TABLE IF EXISTS t_broken_tmp_txn SYNC")
    node.query(
        "CREATE TABLE t_broken_tmp_txn (n Int64) ENGINE = MergeTree ORDER BY n"
        " SETTINGS storage_policy = 'local_object'"
    )
    # A transactional insert persists `txn_version.txt` on the part, so the part is committed and
    # must survive the cleanup of the broken temporary file.
    node.query(
        "INSERT INTO t_broken_tmp_txn VALUES (42)",
        settings={"implicit_transaction": 1, "async_insert": 0},
    )

    part_path = node.query(
        "SELECT path FROM system.parts"
        " WHERE database = 'default' AND table = 't_broken_tmp_txn' AND active LIMIT 1"
    ).strip()
    assert part_path

    ls = node.exec_in_container(["bash", "-c", f"ls {part_path}"])
    assert "txn_version.txt" in ls

    # For a `metadata_type = local` object storage disk, `system.parts.path` is the directory with
    # the metadata files, so the raw write creates a `txn_version.txt.tmp` metadata file that is not
    # even deserializable - like after a hard restart in the middle of writing it.
    node.exec_in_container(
        ["bash", "-c", f"echo incomplete > {part_path}txn_version.txt.tmp"]
    )

    node.restart_clickhouse(kill=True)

    # The server must start, the table must load, and the committed data must survive.
    assert node.query("SELECT count() FROM t_broken_tmp_txn").strip() == "1"
    assert node.query("SELECT n FROM t_broken_tmp_txn").strip() == "42"

    # The leftover file was cleaned up during loading.
    ls = node.exec_in_container(["bash", "-c", f"ls {part_path}"])
    assert "txn_version.txt.tmp" not in ls

    node.query("DROP TABLE t_broken_tmp_txn SYNC")
