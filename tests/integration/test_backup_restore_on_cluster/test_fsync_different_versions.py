import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.test_tools import TSV

# A separate module from test_different_versions.py on purpose: the assertions below make a host
# raise UNKNOWN_SETTING, that error stays in system.errors for the lifetime of the server, and
# system.errors cannot be truncated. test_different_versions asserts system.errors holds nothing
# unexpected, so sharing a server with it would make that test depend on execution order.

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/backups_disk.xml",
    "configs/cluster_different_versions.xml",
]

user_configs = ["configs/user_config.xml"]

new_node = cluster.add_instance(
    "new_node",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "new_node", "shard": "shard1"},
    with_zookeeper=True,
)

old_node = cluster.add_instance(
    "old_node",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "old_node", "shard": "shard1"},
    with_zookeeper=True,
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
        new_node.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster_ver' SYNC")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', 'fsync_ver_{backup_id_counter}')"


def test_fsync_backup_files_not_silently_dropped_on_old_host():
    new_node.query(
        "CREATE TABLE tbl"
        " ON CLUSTER 'cluster_ver'"
        " (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        " ORDER BY tuple()"
    )
    new_node.query("INSERT INTO tbl VALUES (1)")
    old_node.query("INSERT INTO tbl VALUES (2)")

    # new_node is the initiator throughout: it is the only node that serializes backup settings into
    # the internal on-cluster query, and old_node predates fsync_backup_files so it is the receiver
    # being probed. With old_node as initiator the user's own query would be rejected before any
    # serialization happened, which would say nothing about what an initiator forwards.

    # An explicit value of either polarity is a statement about durability, so neither may be
    # dropped on the way to old_node. The value 1 also equals the default, which is precisely the
    # case a serializer that sends only non-default values drops.
    #
    # The assertion is that old_node rejects the setting, read from old_node's own log. It is not
    # that the user's query fails: an old host rejects the internal query before it joins the
    # backup's coordination, so it never writes an error node and the initiator keeps waiting for a
    # host that will never arrive. That unbounded wait is pre-existing and unrelated to this
    # setting - it reproduces on an unpatched server with any backup setting the old host does not
    # know - so it is not asserted here. What matters for durability is that the guarantee is not
    # silently downgraded: old_node refuses the work instead of writing its share without fsync.
    # async = 1 so the initiator returns instead of blocking on the wait described above.
    for value in ["1", "0"]:
        rejections_before = int(
            old_node.count_in_log("Setting fsync_backup_files is neither a builtin")
        )
        new_node.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster_ver' TO {new_backup_name()}"
            f" SETTINGS fsync_backup_files = {value}, async = 1"
        )
        old_node.wait_for_log_line(
            "Setting fsync_backup_files is neither a builtin",
            timeout=60,
            look_behind_lines=2000,
        )
        rejections_after = int(
            old_node.count_in_log("Setting fsync_backup_files is neither a builtin")
        )
        assert rejections_after > rejections_before, (
            f"old_node did not reject fsync_backup_files = {value}:"
            f" rejection count stayed at {rejections_before}"
        )

    # Control for the two assertions above: a setting old_node does know must still work when named
    # explicitly. Without this, both could pass merely because any explicit SETTINGS clause breaks a
    # mixed-version backup.
    new_node.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster_ver' TO {new_backup_name()}"
        " SETTINGS deduplicate_files = 1"
    )

    # The case that must keep working: an unnamed setting stays out of the internal query, so a
    # default cross-version backup still runs. This is what fails if the serialization is widened to
    # force the default value too.
    backup_name = new_backup_name()
    new_node.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster_ver' TO {backup_name}")

    # The restored rows include the one only old_node ever held, so old_node did take part in that
    # backup and the assertion above cannot pass vacuously.
    new_node.query("DROP TABLE tbl ON CLUSTER 'cluster_ver' SYNC")
    new_node.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster_ver' FROM {backup_name}")
    new_node.query("SYSTEM SYNC REPLICA ON CLUSTER 'cluster_ver' tbl")
    assert new_node.query("SELECT * FROM tbl ORDER BY x") == TSV([1, 2])
