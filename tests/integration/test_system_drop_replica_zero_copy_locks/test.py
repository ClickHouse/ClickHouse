#!/usr/bin/env python3

"""
Regression test: SYSTEM DROP REPLICA must release the zero-copy locks the dropped
replica held on every part.

StorageReplicatedMergeTree's zero-copy lock layout is:

    <zero_copy_root>/<part_name>/<uniq_id>/<replica_name>

where <zero_copy_root> is <remote_fs_zero_copy_zookeeper_path>/zero_copy_<disk_type>/<table_shared_id>,
plus <zookeeper_path>/zero_copy_<disk_type>/shared in remote_fs_zero_copy_path_compatible_mode.

Each replica holding a copy of a part creates a leaf named after itself. A live
replica's own unlockSharedDataByID() only frees a part's shared blobs once it
finds no sibling leaves left under <part_name>/<uniq_id>. SYSTEM DROP REPLICA is
meant for a replica that is gone for good and will never run its own
unlockSharedData() to remove its leaf -- before the fix, nothing else did either,
so the leaf stayed forever and the surviving replicas could never free blobs that
only the dropped replica still (nominally) "held a lock" on.

The tests drop a dead replica via both `SYSTEM DROP REPLICA ... FROM TABLE` (a
local StorageReplicatedMergeTree exists) and `SYSTEM DROP REPLICA ... FROM ZKPATH`
(no local table is consulted), and check in each case that:
  * the dropped replica's lock leaf disappears for every part,
  * the surviving replicas' leaves are left untouched,
  * the surviving replicas can still actually free the shared blobs afterwards
    (this is the user-visible symptom: without the fix the blobs leak forever).

The FROM ZKPATH variant is also run from a server that has neither the table nor
the zero-copy disk, with a macro-expanded server-level zero-copy path and in the
compatible mode. Dropping every replica of a table must remove the table's whole
zero-copy subtree together with the last one.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.blobs import list_blobs, wait_blobs_synchronization

cluster = ClickHouseCluster(__file__)

NODES = ("node1", "node2", "node3")

# Has neither the zero-copy disk nor the tables: it can only drop replicas by ZooKeeper path.
BYSTANDER = "node4"

MACROS = {"zc_cluster": "zc"}

# The server-level remote_fs_zero_copy_zookeeper_path from configs/zero_copy_path.xml, with macros expanded.
ZC_ROOT = "/clickhouse/zc/zero_copy/zero_copy_s3"

TABLE_ZC_PATH = "/clickhouse/table_zero_copy"
TABLE_ZC_ROOT = f"{TABLE_ZC_PATH}/zero_copy_s3"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        for name in NODES:
            cluster.add_instance(
                name,
                main_configs=["configs/storage_conf.xml", "configs/zero_copy_path.xml"],
                macros={"replica": name, **MACROS},
                with_minio=True,
                with_zookeeper=True,
                stay_alive=True,
            )
        cluster.add_instance(
            BYSTANDER,
            main_configs=["configs/zero_copy_path.xml"],
            macros={"replica": BYSTANDER, **MACROS},
            with_zookeeper=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _ensure_started(node):
    if node.get_process_pid("clickhouse") is None:
        node.start_clickhouse()


def _create(node, table, zk_path, extra_settings=()):
    settings = "".join(f",\n            {setting}" for setting in extra_settings)
    node.query(
        f"""
        CREATE TABLE {table} (a Int64, b String)
        ENGINE = ReplicatedMergeTree('{zk_path}', '{node.name}')
        ORDER BY a
        SETTINGS
            storage_policy = 's3',
            allow_remote_fs_zero_copy_replication = 1,
            -- make obsolete-part cleanup (which triggers unlockSharedData) fast:
            old_parts_lifetime = 1,
            cleanup_delay_period = 1,
            max_cleanup_delay_period = 1{settings}
        """
    )


def _shared_id(node, zk_path):
    return node.query(
        f"SELECT value FROM system.zookeeper WHERE path = '{zk_path}' AND name = 'table_shared_id'"
    ).strip()


def _children(node, path):
    return [
        name
        for name in node.query(
            f"SELECT name FROM system.zookeeper WHERE path = '{path}'",
            ignore_error=True,
        )
        .strip()
        .splitlines()
        if name
    ]


def _lock_leaves(node, root, part_name):
    """Replica-name leaves under every <part_name>/<uniq_id> directory of a zero-copy root."""
    leaves = []
    for uniq_id in _children(node, f"{root}/{part_name}"):
        leaves += _children(node, f"{root}/{part_name}/{uniq_id}")
    return leaves


def _insert_one_part(nodes, table, zk_path):
    """Insert a single part on the first node and sync the rest. Returns (shared_id, part_name)."""
    nodes[0].query(
        f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(16)"
    )
    for node in nodes[1:]:
        node.query(f"SYSTEM SYNC REPLICA {table}", timeout=30)

    shared_id = _shared_id(nodes[0], zk_path)
    assert shared_id, "table_shared_id not found in ZooKeeper"

    part_name = (
        nodes[0]
        .query(f"SELECT name FROM system.parts WHERE table = '{table}' AND active")
        .strip()
    )
    assert part_name, "no active part found"

    return shared_id, part_name


def _check_drop_replica_releases_locks(
    table, drop_replica_query, executor, extra_settings=(), zc_root=ZC_ROOT, compatible_mode=False
):
    zk_path = f"/clickhouse/tables/{table}"
    node1, node2, node3 = (cluster.instances[n] for n in NODES)

    for n in (node1, node2, node3):
        _ensure_started(n)
        n.query(f"DROP TABLE IF EXISTS {table} SYNC")

    for n in (node1, node2, node3):
        _create(n, table, zk_path, extra_settings)

    # Baseline: table-level blobs (e.g. format_version.txt of each replica) exist,
    # no part blobs yet.
    objects_baseline = list_blobs(cluster.minio_client)

    shared_id, part_name = _insert_one_part((node1, node2, node3), table, zk_path)

    roots = [f"{zc_root}/{shared_id}"]
    if compatible_mode:
        roots.append(f"{zk_path}/zero_copy_s3/shared")

    for root in roots:
        assert set(_lock_leaves(node1, root, part_name)) == set(NODES), (
            f"every replica should hold a zero-copy lock on the part under {root} right after insert+sync"
        )

    try:
        # node3 is gone for good: it will never come back to release its own lock.
        node3.stop_clickhouse()
        cluster.instances[executor].query(drop_replica_query)

        # node3's leaf must be gone; node1/node2's own leaves must be untouched.
        for root in roots:
            assert set(_lock_leaves(node1, root, part_name)) == {"node1", "node2"}, (
                f"SYSTEM DROP REPLICA must remove exactly the dropped replica's own lock leaf under {root}"
            )

        # The two remaining live replicas now drop the part: the part blobs must
        # actually get freed, which only happens if unlockSharedDataByID() no longer
        # sees node3's phantom lock. Without the fix the leaf survives and the blobs
        # leak forever, so this wait fails.
        node1.query(f"TRUNCATE TABLE {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}", timeout=30)
        wait_blobs_synchronization(cluster.minio_client, objects_baseline)
    finally:
        _ensure_started(node3)

    for n in (node1, node2, node3):
        n.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_system_drop_replica_from_table_releases_zero_copy_locks(started_cluster):
    # FROM TABLE takes the zero-copy path from the local table, so a per-table override is found.
    table = "zc_drop_replica_from_table"
    _check_drop_replica_releases_locks(
        table,
        f"SYSTEM DROP REPLICA 'node3' FROM TABLE {table}",
        executor="node1",
        extra_settings=[f"remote_fs_zero_copy_zookeeper_path = '{TABLE_ZC_PATH}'"],
        zc_root=TABLE_ZC_ROOT,
    )


def test_system_drop_replica_from_zkpath_releases_zero_copy_locks(started_cluster):
    # No local table is consulted: the zero-copy root comes from the server-level,
    # macro-expanded remote_fs_zero_copy_zookeeper_path.
    table = "zc_drop_replica_from_zkpath"
    _check_drop_replica_releases_locks(
        table,
        f"SYSTEM DROP REPLICA 'node3' FROM ZKPATH '/clickhouse/tables/{table}'",
        executor="node1",
    )


def test_system_drop_replica_from_zkpath_on_bystander_releases_zero_copy_locks(started_cluster):
    # The server running the query has no zero-copy disk, so the disk types must be taken
    # from ZooKeeper. The compatible mode adds the legacy root under the table's own path.
    table = "zc_drop_replica_from_zkpath_bystander"
    _check_drop_replica_releases_locks(
        table,
        f"SYSTEM DROP REPLICA 'node3' FROM ZKPATH '/clickhouse/tables/{table}'",
        executor=BYSTANDER,
        extra_settings=["remote_fs_zero_copy_path_compatible_mode = 1"],
        compatible_mode=True,
    )


def test_system_drop_last_replica_drops_zero_copy_root(started_cluster):
    # The table exists only on node2 and node3; the bystander cleans up the dead replicas
    # by ZooKeeper path. Dropping the last replica must remove the table's entire
    # zero-copy subtree, not only the dropped replicas' leaves.
    table = "zc_drop_last_replica"
    zk_path = f"/clickhouse/tables/{table}"
    node1, node2, node3 = (cluster.instances[n] for n in NODES)
    bystander = cluster.instances[BYSTANDER]

    for n in (node1, node2, node3):
        _ensure_started(n)
        n.query(f"DROP TABLE IF EXISTS {table} SYNC")

    _create(node2, table, zk_path)
    _create(node3, table, zk_path)

    shared_id, part_name = _insert_one_part((node2, node3), table, zk_path)
    root = f"{ZC_ROOT}/{shared_id}"
    assert set(_lock_leaves(bystander, root, part_name)) == {"node2", "node3"}

    try:
        node3.stop_clickhouse()
        bystander.query(f"SYSTEM DROP REPLICA 'node3' FROM ZKPATH '{zk_path}'")
        assert set(_lock_leaves(bystander, root, part_name)) == {"node2"}

        node2.stop_clickhouse()
        bystander.query(f"SYSTEM DROP REPLICA 'node2' FROM ZKPATH '{zk_path}'")

        # The last replica is gone: the table's nodes and its whole zero-copy subtree
        # must be gone with it.
        assert shared_id not in _children(bystander, ZC_ROOT), (
            "the table's zero-copy subtree must be removed together with its last replica"
        )
        assert table not in _children(bystander, "/clickhouse/tables"), (
            "the table's own ZooKeeper nodes must be removed together with its last replica"
        )
    finally:
        _ensure_started(node2)
        _ensure_started(node3)

    # The tables left on node2/node3 point to removed ZooKeeper metadata; dropping
    # them must still work (and cleans up their local data).
    for n in (node2, node3):
        n.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_system_drop_replica_from_zkpath_warns_about_per_table_zero_copy_path(started_cluster):
    # A per-table remote_fs_zero_copy_zookeeper_path is not recorded in ZooKeeper, so FROM ZKPATH
    # cannot find the locks under it, and must say so instead of silently succeeding.
    table = "zc_drop_replica_per_table_path"
    zk_path = f"/clickhouse/tables/{table}"
    node1, node2, node3 = (cluster.instances[n] for n in NODES)
    bystander = cluster.instances[BYSTANDER]

    for n in (node1, node2, node3):
        _ensure_started(n)
        n.query(f"DROP TABLE IF EXISTS {table} SYNC")

    for n in (node1, node2, node3):
        _create(n, table, zk_path, [f"remote_fs_zero_copy_zookeeper_path = '{TABLE_ZC_PATH}'"])

    _insert_one_part((node1, node2, node3), table, zk_path)

    try:
        node3.stop_clickhouse()
        bystander.query(f"SYSTEM DROP REPLICA 'node3' FROM ZKPATH '{zk_path}'")
        assert bystander.contains_in_log(f"No zero-copy locks of table {zk_path} were found")
    finally:
        _ensure_started(node3)

    for n in (node1, node2, node3):
        n.query(f"DROP TABLE IF EXISTS {table} SYNC")
