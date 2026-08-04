import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_s3"
NUM_ROWS = 1000


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node",
        main_configs=["configs/storage_conf.xml"],
        with_rustfs=True,
        stay_alive=True,
    )

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cas_s3():
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS cas_test SYNC")
    node.query(
        """
        CREATE TABLE cas_test (
            id Int64,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = '{}'
        """.format(
            STORAGE_POLICY
        )
    )

    # First insert of NUM_ROWS deterministic rows.
    node.query(
        "INSERT INTO cas_test SELECT number, toString(number) FROM numbers({})".format(
            NUM_ROWS
        )
    )

    expected_sum = (NUM_ROWS - 1) * NUM_ROWS // 2
    assert int(node.query("SELECT count() FROM cas_test")) == NUM_ROWS
    assert int(node.query("SELECT sum(id) FROM cas_test")) == expected_sum

    # A second identical insert: the row count doubles. Each part's content is identical, so the
    # content-addressed disk deduplicates the blobs, but the logical row count must still double.
    node.query(
        "INSERT INTO cas_test SELECT number, toString(number) FROM numbers({})".format(
            NUM_ROWS
        )
    )
    assert int(node.query("SELECT count() FROM cas_test")) == 2 * NUM_ROWS
    assert int(node.query("SELECT sum(id) FROM cas_test")) == 2 * expected_sum

    # Merge the two parts together.
    node.query("OPTIMIZE TABLE cas_test FINAL")
    assert int(node.query("SELECT count() FROM cas_test")) == 2 * NUM_ROWS
    assert int(node.query("SELECT sum(id) FROM cas_test")) == 2 * expected_sum

    # Persistence: after a restart the refs/footers/blobs in S3 must still resolve the data.
    node.restart_clickhouse()

    assert int(node.query("SELECT count() FROM cas_test")) == 2 * NUM_ROWS
    assert int(node.query("SELECT sum(id) FROM cas_test")) == 2 * expected_sum

    # Drop must complete without error (ref unlink + deferred GC).
    node.query("DROP TABLE cas_test SYNC")
    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'cas_test'"
        ).strip()
        == "0"
    )


def test_mutations_and_patch_parts_survive_restart():
    # A mutated part and a patch part are ordinary content-addressed parts published as refs. After a
    # restart the active set must be rediscovered from the refs in S3, so the post-mutation /
    # post-lightweight-delete state must survive (CAS M7).
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS cas_mut SYNC")
    node.query(
        """
        CREATE TABLE cas_mut (
            id Int64,
            v UInt64,
            s String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = '{}', enable_block_number_column = 1, enable_block_offset_column = 1
        """.format(
            STORAGE_POLICY
        )
    )

    node.query(
        "INSERT INTO cas_mut SELECT number, number * 10, toString(number) FROM numbers({})".format(
            NUM_ROWS
        )
    )

    # Heavy mutation: UPDATE one column (id/s carry forward by reference on the content-addressed disk).
    node.query(
        "ALTER TABLE cas_mut UPDATE v = v + 1 WHERE id % 2 = 0 SETTINGS mutations_sync = 2"
    )
    # Heavy mutation: DELETE.
    node.query("ALTER TABLE cas_mut DELETE WHERE id % 100 = 0 SETTINGS mutations_sync = 2")
    # Data-ALTER (column type change). Via a storage policy there is no inline-disk CustomType in
    # settings_changes, so this works on the content-addressed disk (see backlog B53).
    node.query("ALTER TABLE cas_mut MODIFY COLUMN v Int64 SETTINGS mutations_sync = 2")
    # Patch part: a forced lightweight-update DELETE (throws if unsupported, so success == patch path).
    node.query(
        "DELETE FROM cas_mut WHERE id % 7 = 0 "
        "SETTINGS enable_lightweight_update = 1, lightweight_delete_mode = 'lightweight_update_force', lightweight_deletes_sync = 2"
    )

    count_before = int(node.query("SELECT count() FROM cas_mut"))
    sum_before = int(node.query("SELECT sum(v) FROM cas_mut"))
    digest_before = node.query("SELECT sum(cityHash64(id, v, s)) FROM cas_mut").strip()

    # Persistence: rediscover the active set (incl. the mutated and patch parts) from S3 refs.
    node.restart_clickhouse()

    assert int(node.query("SELECT count() FROM cas_mut")) == count_before
    assert int(node.query("SELECT sum(v) FROM cas_mut")) == sum_before
    assert node.query("SELECT sum(cityHash64(id, v, s)) FROM cas_mut").strip() == digest_before

    node.query("DROP TABLE cas_mut SYNC")
    assert (
        node.query(
            "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'cas_mut'"
        ).strip()
        == "0"
    )
