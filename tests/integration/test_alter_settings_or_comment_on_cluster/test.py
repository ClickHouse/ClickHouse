import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        ch1.query("CREATE DATABASE test_db ON CLUSTER 'cluster'")
        yield cluster
    finally:
        cluster.shutdown()


def get_zk_metadata_version(node, zookeeper_path):
    # The ZooKeeper `/metadata` node version is only bumped by the replicated
    # ALTER_METADATA path. A local settings/comment fast path leaves it
    # untouched, so it is a precise signal for "this ALTER did not write a
    # replicated log entry".
    return int(
        node.query(
            f"SELECT version FROM system.zookeeper WHERE path = '{zookeeper_path}' AND name = 'metadata'"
        ).strip()
    )


def list_part_index_files(node, database, table, part_name):
    # The on-disk secondary index filename is the materialisation of the in-memory
    # IndexDescription::escape_filenames flag (escaped: skp_idx_<escaped>.idx,
    # unescaped: skp_idx_<raw>.idx), so listing it is how we observe the index
    # filename policy actually in effect on a replica without restarting it.
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = '{database}' AND name = '{table}'"
    ).strip()
    files = node.exec_in_container(
        ["bash", "-c", f"ls {data_path}/{part_name}/ | grep '^skp_idx_' || true"]
    )
    # The skip-index data file is .idx or .idx2 depending on the part format version;
    # the basename before the extension carries the escaping we care about.
    return sorted(f for f in files.splitlines() if f.endswith((".idx", ".idx2")))


def wait_show_create(
    node, table, *, contains=(), not_contains=(), retries=60, sleep_time=0.5
):
    # An ON CLUSTER ALTER returns once the distributed DDL task is finished on every
    # host, which does not guarantee the change is already visible in that host's
    # in-memory metadata: the replicated ALTER_METADATA entry (or the local
    # settings/comment fast path) can still be in flight, especially right after a
    # replica restart. Poll SHOW CREATE until it converges; a value that never
    # converges still fails the caller's assert with the final SHOW CREATE.
    show_create = ""
    for _ in range(retries):
        show_create = node.query(
            database="test_db", sql=f"SHOW CREATE {table} FORMAT TSVRaw"
        )
        if all(s in show_create for s in contains) and all(
            s not in show_create for s in not_contains
        ):
            break
        time.sleep(sleep_time)
    return show_create


def wait_column_order(node, table, expected, *, retries=60, sleep_time=0.5):
    # Same convergence reasoning as wait_show_create, for the column order read from
    # system.columns after an ON CLUSTER MODIFY COLUMN with a positional modifier.
    order = []
    for _ in range(retries):
        order = node.query(
            database="test_db",
            sql=f"SELECT name FROM system.columns WHERE database = 'test_db' AND table = '{table}' ORDER BY position FORMAT TSVRaw",
        ).split()
        if order == expected:
            break
        time.sleep(sleep_time)
    return order


def test_mixed_settings_and_comment_alter_on_cluster(started_cluster):
    # ON CLUSTER ALTER batches mixing MODIFY SETTING / RESET SETTING with
    # column or table comments must converge on every replica. The storage
    # layer applies settings/comments as local metadata (never via the
    # replicated log), so convergence relies on DDLWorker forwarding the
    # query to all replicas rather than just the leader.
    zookeeper_path = "/clickhouse/tables/mixed_alter"
    ch1.query(
        database="test_db",
        sql=f"CREATE TABLE mixed_alter (x UInt64) ENGINE=ReplicatedMergeTree('{zookeeper_path}', 'r1') ORDER BY tuple()",
    )
    ch2.query(
        database="test_db",
        sql=f"CREATE TABLE mixed_alter (x UInt64) ENGINE=ReplicatedMergeTree('{zookeeper_path}', 'r2') ORDER BY tuple()",
    )

    # MODIFY COMMENT + MODIFY SETTING in a single ON CLUSTER ALTER.
    version_before = get_zk_metadata_version(ch1, zookeeper_path)
    ch1.query(
        database="test_db",
        sql="ALTER TABLE mixed_alter ON CLUSTER 'cluster' MODIFY COMMENT 'mixed-on-cluster', MODIFY SETTING old_parts_lifetime = 123",
    )

    # The mixed batch must take the local fast path on every replica, not the
    # replicated ALTER_METADATA path. Otherwise both replicas race for the same
    # /metadata version, one wins and the others retry with CANNOT_ASSIGN_ALTER
    # while appending no-op log entries. The ZK metadata version must not move.
    assert get_zk_metadata_version(ch1, zookeeper_path) == version_before

    # Both replicas must see the new comment and the new setting value.
    for node in [ch1, ch2]:
        show_create = wait_show_create(
            node, "mixed_alter", contains=["old_parts_lifetime = 123", "mixed-on-cluster"]
        )
        assert "old_parts_lifetime = 123" in show_create, (node.name, show_create)
        assert "mixed-on-cluster" in show_create, (node.name, show_create)

    # MODIFY COMMENT + RESET SETTING - same shape, mixed setting / comment kinds.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE mixed_alter ON CLUSTER 'cluster' MODIFY COMMENT 'second-mixed', RESET SETTING old_parts_lifetime",
    )

    for node in [ch1, ch2]:
        show_create = wait_show_create(
            node,
            "mixed_alter",
            contains=["second-mixed"],
            not_contains=["old_parts_lifetime = 123"],
        )
        assert "old_parts_lifetime = 123" not in show_create, (node.name, show_create)
        assert "second-mixed" in show_create, (node.name, show_create)

    # COMMENT COLUMN + MODIFY SETTING - column-comment variant.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE mixed_alter ON CLUSTER 'cluster' COMMENT COLUMN x 'x-col-comment', MODIFY SETTING old_parts_lifetime = 234",
    )

    for node in [ch1, ch2]:
        show_create = wait_show_create(
            node, "mixed_alter", contains=["old_parts_lifetime = 234", "x-col-comment"]
        )
        assert "old_parts_lifetime = 234" in show_create, (node.name, show_create)
        assert "x-col-comment" in show_create, (node.name, show_create)

    ch1.query(
        database="test_db",
        sql="DROP TABLE mixed_alter ON CLUSTER 'cluster' SYNC",
    )


def test_modify_column_comment_only_on_cluster(started_cluster):
    # A pure `ALTER ... MODIFY COLUMN c COMMENT 'x'` parses as `MODIFY_COLUMN` but
    # the storage layer recognises it as comment-only and applies it as local
    # metadata, so it must also be routed to every replica. A MODIFY COLUMN that
    # carries a type change, a positional modifier (FIRST/AFTER) or per-column
    # SETTINGS is NOT comment-only and must take the full replicated-log path
    # (negative cases below). All must converge across replicas.
    zookeeper_path = "/clickhouse/tables/modcol_comment"
    ch1.query(
        database="test_db",
        sql=f"CREATE TABLE modcol_comment (id UInt64, x String) ENGINE=ReplicatedMergeTree('{zookeeper_path}', 'r1') ORDER BY id",
    )
    ch2.query(
        database="test_db",
        sql=f"CREATE TABLE modcol_comment (id UInt64, x String) ENGINE=ReplicatedMergeTree('{zookeeper_path}', 'r2') ORDER BY id",
    )

    # `MODIFY COLUMN ... COMMENT '...'` only: comment-only, local fast path on
    # every replica, ZK /metadata version must not move.
    version_before = get_zk_metadata_version(ch1, zookeeper_path)
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x COMMENT 'modcol-comment-v1'",
    )

    assert get_zk_metadata_version(ch1, zookeeper_path) == version_before
    for node in [ch1, ch2]:
        show_create = wait_show_create(
            node, "modcol_comment", contains=["modcol-comment-v1"]
        )
        assert "modcol-comment-v1" in show_create, (node.name, show_create)

    # Mixed: `MODIFY COLUMN ... COMMENT '...'` + `MODIFY SETTING`.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x COMMENT 'modcol-comment-v2', MODIFY SETTING old_parts_lifetime = 345",
    )

    for node in [ch1, ch2]:
        show_create = wait_show_create(
            node,
            "modcol_comment",
            contains=["modcol-comment-v2", "old_parts_lifetime = 345"],
        )
        assert "modcol-comment-v2" in show_create, (node.name, show_create)
        assert "old_parts_lifetime = 345" in show_create, (node.name, show_create)

    # Mixed: `MODIFY COLUMN ... COMMENT '...'` + `MODIFY COMMENT '...'`.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x COMMENT 'modcol-comment-v3', MODIFY COMMENT 'table-comment-modcol'",
    )

    for node in [ch1, ch2]:
        show_create = wait_show_create(
            node,
            "modcol_comment",
            contains=["modcol-comment-v3", "table-comment-modcol"],
        )
        assert "modcol-comment-v3" in show_create, (node.name, show_create)
        assert "table-comment-modcol" in show_create, (node.name, show_create)

    # Positional `MODIFY COLUMN ... COMMENT '...' FIRST`: a placement modifier
    # reorders the column, which is serialized into the replicated /columns
    # (ColumnsDescription::operator== compares column order). It is therefore NOT
    # comment-only and must take the full replicated path: the ZK /metadata
    # version moves, /columns is rewritten in ZooKeeper, and every replica picks
    # up the new order through the replication log. If it were wrongly applied via
    # the local comment fast path, the leader would reorder its local columns
    # without updating ZK /columns and throw INCOMPATIBLE_COLUMNS on restart.
    version_before = get_zk_metadata_version(ch1, zookeeper_path)
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x COMMENT 'modcol-placement-comment' FIRST",
    )

    assert get_zk_metadata_version(ch1, zookeeper_path) > version_before
    for node in [ch1, ch2]:
        show_create = wait_show_create(
            node, "modcol_comment", contains=["modcol-placement-comment"]
        )
        assert "modcol-placement-comment" in show_create, (node.name, show_create)
        order = wait_column_order(node, "modcol_comment", ["x", "id"])
        assert order == ["x", "id"], (node.name, order)

    # Restart both replicas: the local columns (reordered to x, id) must match the
    # ZK /columns written by the replicated ALTER, so the table loads without
    # INCOMPATIBLE_COLUMNS and the order survives the restart.
    for node in [ch1, ch2]:
        node.restart_clickhouse()
    for node in [ch1, ch2]:
        order = wait_column_order(node, "modcol_comment", ["x", "id"])
        assert order == ["x", "id"], (node.name, order)
        show_create = wait_show_create(
            node, "modcol_comment", contains=["modcol-placement-comment"]
        )
        assert "modcol-placement-comment" in show_create, (node.name, show_create)

    # Negative case: `MODIFY COLUMN` with a real type change must still route as a
    # full replicated ALTER and converge across replicas through the replication log.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x String COMMENT 'modcol-with-type'",
    )

    for node in [ch1, ch2]:
        show_create = wait_show_create(
            node, "modcol_comment", contains=["modcol-with-type"]
        )
        assert "modcol-with-type" in show_create, (node.name, show_create)

    ch1.query(
        database="test_db",
        sql="DROP TABLE modcol_comment ON CLUSTER 'cluster' SYNC",
    )


def test_mixed_setting_escape_index_filenames_on_cluster(started_cluster):
    # escape_index_filenames is a setting whose value drives derived metadata:
    # changeSettings rewrites StorageInMemoryMetadata::escape_index_filenames and the
    # per-index escape_filenames flag, which controls the on-disk index filename. A
    # mixed `MODIFY SETTING escape_index_filenames = 0, MODIFY COMMENT` batch must apply
    # that derived metadata locally on every replica, not just record the setting in
    # SHOW CREATE. The index name has a non-word character so escaping changes the file.
    zookeeper_path = "/clickhouse/tables/escape_mixed"
    # PARTITION BY x so each replica writes its own part in its own partition; a part
    # keeps the index filenames of whoever wrote it, even after replication fetches it,
    # so inspecting a node's own partition isolates that node's in-memory policy.
    create_sql = (
        "CREATE TABLE escape_mixed (x UInt64, INDEX `idx-esc` x TYPE minmax GRANULARITY 1) "
        "ENGINE=ReplicatedMergeTree('{zk}', '{replica}') ORDER BY x PARTITION BY x "
        "SETTINGS escape_index_filenames = 1, min_bytes_for_wide_part = 0"
    )
    ch1.query(database="test_db", sql=create_sql.format(zk=zookeeper_path, replica="r1"))
    ch2.query(database="test_db", sql=create_sql.format(zk=zookeeper_path, replica="r2"))

    # Mixed batch: turn escaping off and set a comment in one ON CLUSTER ALTER.
    # MODIFY COMMENT is placed first because MODIFY SETTING parses a comma-separated
    # list, so a trailing comma after it would be read as another setting change.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE escape_mixed ON CLUSTER 'cluster' MODIFY COMMENT 'escape-mixed', MODIFY SETTING escape_index_filenames = 0",
    )

    # Each replica writes a part locally (distinct partition) after the alter, so the
    # new part uses whatever index filename policy is actually in memory on that replica.
    # With escaping disabled, the unescaped index file (skp_idx_idx-esc.idx) must be
    # written; a replica that reverted to the old policy would write skp_idx_idx%2Desc.idx.
    for value, node in [(1, ch1), (2, ch2)]:
        node.query(database="test_db", sql=f"INSERT INTO escape_mixed VALUES ({value})")
        part_name = node.query(
            database="test_db",
            sql=f"SELECT name FROM system.parts WHERE database = 'test_db' AND table = 'escape_mixed' AND active = 1 AND partition = '{value}'",
        ).strip()
        index_files = list_part_index_files(node, "test_db", "escape_mixed", part_name)
        # Escaping disabled -> the raw index name (idx-esc) appears in the filename.
        # If a replica reverted to escaping, the name would be skp_idx_idx%2Desc.idx*.
        assert len(index_files) == 1 and index_files[0].startswith("skp_idx_idx-esc."), (
            node.name,
            index_files,
        )
        show_create = wait_show_create(
            node, "escape_mixed", contains=["escape_index_filenames = 0", "escape-mixed"]
        )
        assert "escape_index_filenames = 0" in show_create, (node.name, show_create)
        assert "escape-mixed" in show_create, (node.name, show_create)

    ch1.query(
        database="test_db",
        sql="DROP TABLE escape_mixed ON CLUSTER 'cluster' SYNC",
    )
