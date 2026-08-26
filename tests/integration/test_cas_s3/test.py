import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_s3"
NUM_ROWS = 1000
CAS_PUBLICATION_EVENTS = (
    "CASBlobBodyPutAvoided",
    "CASBlobHead",
    "CASBlobHeadMiss",
    "CASBlobPut",
    "CASBlobUploadFanoutTasks",
    "CASMetaCreateClean",
)


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


def cas_publication_events(node):
    """Return process-wide CAS publication counters for an isolated before/after budget."""
    rows = node.query(
        "SELECT event, value FROM system.events WHERE event IN ({}) FORMAT TSV".format(
            ", ".join("'{}'".format(event) for event in CAS_PUBLICATION_EVENTS)
        )
    )
    values = {event: 0 for event in CAS_PUBLICATION_EVENTS}
    for row in rows.splitlines():
        event, value = row.split("\t")
        values[event] = int(value)
    return values


def event_delta(before, after):
    return {event: after[event] - before[event] for event in CAS_PUBLICATION_EVENTS}


def test_disk_accepts_backend_settings_that_used_to_be_rejected():
    """The CAS disk block carries settings of its underlying object storage.

    Before the `cas_` namespace, the CAS settings scanned the whole disk element and rejected every
    key they did not recognise, so `http_keep_alive_timeout` -- the mitigation suggested in #2243 --
    failed server startup. The server having started with the config this module installs is most of
    the proof; this test states it, and checks the disk is actually usable rather than merely
    present.
    """
    node = cluster.instances["node"]
    assert node.query(
        "SELECT count() FROM system.disks WHERE name = 'disk_cas_s3'"
    ).strip() == "1"
    node.query("DROP TABLE IF EXISTS t_foreign_settings SYNC")
    node.query(
        "CREATE TABLE t_foreign_settings (a UInt64) ENGINE = MergeTree ORDER BY a "
        "SETTINGS storage_policy = '{}'".format(STORAGE_POLICY)
    )
    node.query("INSERT INTO t_foreign_settings SELECT number FROM numbers(100)")
    assert node.query("SELECT sum(a) FROM t_foreign_settings").strip() == "4950"
    node.query("DROP TABLE t_foreign_settings SYNC")


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

    # First insert of NUM_ROWS deterministic rows. The RustFS lane has no concurrent query writer,
    # so process-wide ProfileEvents form an exact request budget for this operation.
    before_fresh = cas_publication_events(node)
    node.query(
        "INSERT INTO cas_test SELECT number, toString(number) FROM numbers({})".format(
            NUM_ROWS
        )
    )
    fresh = event_delta(before_fresh, cas_publication_events(node))
    assert fresh["CASBlobUploadFanoutTasks"] > 0, fresh
    assert (
        fresh["CASBlobHead"] + fresh["CASBlobHeadMiss"]
        == fresh["CASBlobUploadFanoutTasks"]
    ), fresh
    assert fresh["CASBlobHeadMiss"] == fresh["CASBlobUploadFanoutTasks"], fresh
    assert fresh["CASBlobBodyPutAvoided"] == 0, fresh
    assert fresh["CASMetaCreateClean"] == fresh["CASBlobUploadFanoutTasks"], fresh
    # `CASBlobPut` is namespace/path instrumentation: both the body and its `.meta` sibling live
    # below `/blobs/`, so a fresh publication contributes exactly those two physical PUTs.
    assert (
        fresh["CASBlobPut"]
        == fresh["CASBlobUploadFanoutTasks"] + fresh["CASMetaCreateClean"]
    ), fresh

    expected_sum = (NUM_ROWS - 1) * NUM_ROWS // 2
    assert int(node.query("SELECT count() FROM cas_test")) == NUM_ROWS
    assert int(node.query("SELECT sum(id) FROM cas_test")) == expected_sum

    # A second identical insert: the row count doubles. Each part's content is identical, so the
    # content-addressed disk deduplicates the blobs, but the logical row count must still double.
    before_duplicate = cas_publication_events(node)
    node.query(
        "INSERT INTO cas_test SELECT number, toString(number) FROM numbers({})".format(
            NUM_ROWS
        )
    )
    duplicate = event_delta(before_duplicate, cas_publication_events(node))
    assert duplicate["CASBlobUploadFanoutTasks"] > 0, duplicate
    assert (
        duplicate["CASBlobHead"] + duplicate["CASBlobHeadMiss"]
        == duplicate["CASBlobUploadFanoutTasks"]
    ), duplicate
    assert duplicate["CASBlobHead"] == duplicate["CASBlobUploadFanoutTasks"], duplicate
    assert duplicate["CASBlobHeadMiss"] == 0, duplicate
    assert duplicate["CASBlobPut"] == 0, duplicate
    assert duplicate["CASBlobBodyPutAvoided"] == duplicate["CASBlobHead"], duplicate
    assert duplicate["CASMetaCreateClean"] == 0, duplicate
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
