import time

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
    # The assertions below leave a backup that can never complete, and the server waits for
    # unfinished backups on shutdown by default.
    "configs/shutdown_cancel_backups.xml",
]

user_configs = [
    "configs/user_config.xml",
    "configs/finite_backup_error_timeout.xml",
]

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


# How many times old_node has refused the setting. Counted rather than waited for as a log line,
# because a wait for the text is satisfied by a line an earlier assertion produced.
def count_rejections():
    return int(
        old_node.count_in_log("Setting fsync_backup_files is neither a builtin")
    )


def assert_eventually(predicate, message, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(1)
    raise AssertionError(message)


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
    # What is asserted is that old_node refuses the setting, not that the user's query fails: an old
    # host rejects the internal query before it joins the backup's coordination, so it writes no
    # error node and the initiator keeps waiting for a host that never arrives. That unbounded wait
    # is pre-existing and independent of this setting, so async = 1 is used to step around it.
    for value in ["1", "0"]:
        rejections_before = count_rejections()
        backup_id = f"fsync_ver_explicit_{value}"
        new_node.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster_ver' TO {new_backup_name()}"
            f" SETTINGS fsync_backup_files = {value}, async = 1, id = '{backup_id}'"
        )
        assert_eventually(
            lambda before=rejections_before: count_rejections() > before,
            f"old_node did not reject fsync_backup_files = {value}:"
            f" rejection count stayed at {rejections_before}",
        )
        # The backup can never finish: old_node refused the internal query before joining the
        # coordination, so nothing will ever release the initiator's wait for it. Cancel it here so
        # it does not outlive the test. Asynchronously, without SYNC: SYNC polls until the killed
        # query is gone, which is the very thing that cannot happen here.
        new_node.query(
            "KILL QUERY WHERE (query_kind = 'Backup')"
            f" AND (query LIKE '%{backup_id}%') ASYNC"
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
