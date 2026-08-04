-- Tags: no-fasttest, no-shared-merge-tree
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.
--   no-shared-merge-tree: this exercises the open-source ReplicatedMergeTree path.

-- B33 (lifted): ReplicatedMergeTree on a cas disk is now SUPPORTED. The earlier
-- SUPPORT_IS_DISABLED gate in StorageReplicatedMergeTree was removed once replication-internal clones
-- stopped corrupting content-addressed parts, so creating a ReplicatedMergeTree table on a
-- cas disk now succeeds and works end-to-end. A plain (non-replicated) MergeTree on the
-- same kind of disk must also still work.

DROP TABLE IF EXISTS t_cas_repl;
DROP TABLE IF EXISTS t_cas_plain;

-- (1) A ReplicatedMergeTree table on a cas disk is now supported end-to-end.
CREATE TABLE t_cas_repl (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_cas_repl', 'r1')
ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04283',
    name = '04283_cas_repl',
    path = '04283_cas_repl_pool/');

INSERT INTO t_cas_repl VALUES (1, 10), (2, 20);
SELECT 'repl_count', count() FROM t_cas_repl;
SELECT 'repl_sum', sum(b) FROM t_cas_repl;
DROP TABLE t_cas_repl;

-- (2) A plain (non-replicated) MergeTree on a cas disk still works end-to-end.
CREATE TABLE t_cas_plain (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04283',
    name = '04283_cas_plain',
    path = '04283_cas_plain_pool/');

INSERT INTO t_cas_plain SELECT number, number * 2 FROM numbers(100);
SELECT 'plain_count', count() FROM t_cas_plain;
SELECT 'plain_sum', sum(b) FROM t_cas_plain;

DROP TABLE t_cas_plain;
SELECT 'dropped_ok';
