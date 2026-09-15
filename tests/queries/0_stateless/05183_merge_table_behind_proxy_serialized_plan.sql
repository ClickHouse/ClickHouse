-- A read of a deserialized query plan carries no analyzed query, so it may only be routed straight to
-- a storage that does not need one. Reading a Merge table through a proxy that forwards its read used
-- to take that route and crash the server.
-- The lazily loaded `StorageTableProxy` carrier needs a second database, so it lives in
-- 05183_merge_table_behind_proxy_serialized_plan_2, which is tagged `no-replicated-database`.

DROP TABLE IF EXISTS t_merge_proxy_alias;
DROP TABLE IF EXISTS t_merge_proxy_buffer;
DROP VIEW IF EXISTS t_merge_proxy_mv;
DROP TABLE IF EXISTS t_merge_proxy_merge;
DROP TABLE IF EXISTS t_merge_proxy_src;

CREATE TABLE t_merge_proxy_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_merge_proxy_src VALUES (1), (2);

CREATE TABLE t_merge_proxy_merge ENGINE = Merge(currentDatabase(), '^t_merge_proxy_src$');
CREATE TABLE t_merge_proxy_alias ENGINE = Alias(currentDatabase(), t_merge_proxy_merge);
CREATE TABLE t_merge_proxy_buffer (x UInt64)
    ENGINE = Buffer(currentDatabase(), t_merge_proxy_merge, 1, 100, 100, 10000, 1000000, 10000000, 100000000);
CREATE MATERIALIZED VIEW t_merge_proxy_mv TO t_merge_proxy_merge AS SELECT x FROM t_merge_proxy_src;

SET enable_analyzer = 1, serialize_query_plan = 1;

SELECT 'alias', sum(x) FROM remote('127.0.0.2', currentDatabase(), t_merge_proxy_alias);
SELECT 'buffer', sum(x) FROM remote('127.0.0.2', currentDatabase(), t_merge_proxy_buffer);
SELECT 'mv', sum(x) FROM remote('127.0.0.2', currentDatabase(), t_merge_proxy_mv);

-- With these two on, the same read reaches an earlier unguarded dereference of the same null query.
SELECT 'alias, plan-based parallel replicas', sum(x)
FROM remote('127.0.0.2', currentDatabase(), t_merge_proxy_alias)
SETTINGS parallel_replicas_plan_based = 1, parallel_replicas_allow_merge_tables = 1;

-- A directly addressed Merge table was already routed correctly, by an exact type test that this
-- fix replaces; it has to keep working.
SELECT 'merge', sum(x) FROM remote('127.0.0.2', currentDatabase(), t_merge_proxy_merge);

DROP TABLE t_merge_proxy_alias;
DROP TABLE t_merge_proxy_buffer;
DROP VIEW t_merge_proxy_mv;
DROP TABLE t_merge_proxy_merge;
DROP TABLE t_merge_proxy_src;
