-- Tests that the view transparency of `modification_hash` (issue #108713) lives in the storage-level
-- `getModificationHash`, not only in the query-cache helper: `system.tables.modification_hash` must be
-- non-NULL for a `View` and a `MaterializedView` over supported engines, must track the tables behind
-- the view, and wrapper engines reading a view (`Merge`, local `Distributed`) must keep working instead
-- of silently failing closed.

DROP TABLE IF EXISTS t_04695;
DROP TABLE IF EXISTS v_04695;
DROP TABLE IF EXISTS mv_src_04695;
DROP TABLE IF EXISTS mv_04695;
DROP TABLE IF EXISTS merge_over_view_04695;
DROP TABLE IF EXISTS dist_over_view_04695;

CREATE TABLE t_04695 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04695 VALUES (1);

CREATE VIEW v_04695 AS SELECT x FROM t_04695;

CREATE TABLE mv_src_04695 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_04695 ENGINE = MergeTree ORDER BY x AS SELECT x FROM mv_src_04695;
INSERT INTO mv_src_04695 VALUES (1);

-- The storage-level column is populated for both view kinds.
SELECT 'view not null', modification_hash IS NOT NULL FROM system.tables WHERE database = currentDatabase() AND name = 'v_04695';
SELECT 'materialized view not null', modification_hash IS NOT NULL FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04695';

-- The value tracks the table behind the view: an INSERT into the base table changes it.
DROP TABLE IF EXISTS hashes_04695;
CREATE TABLE hashes_04695 (name String, hash UInt128) ENGINE = Memory;

INSERT INTO hashes_04695 SELECT 'v_before', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'v_04695';
INSERT INTO hashes_04695 SELECT 'mv_before', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04695';

INSERT INTO t_04695 VALUES (2);
INSERT INTO mv_src_04695 VALUES (2);

INSERT INTO hashes_04695 SELECT 'v_after', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'v_04695';
INSERT INTO hashes_04695 SELECT 'mv_after', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04695';

SELECT 'view hash changed', (SELECT hash FROM hashes_04695 WHERE name = 'v_before') != (SELECT hash FROM hashes_04695 WHERE name = 'v_after');
SELECT 'materialized view hash changed', (SELECT hash FROM hashes_04695 WHERE name = 'mv_before') != (SELECT hash FROM hashes_04695 WHERE name = 'mv_after');

-- A wrapper engine over a view recurses into the view's hash instead of failing closed.
CREATE TABLE merge_over_view_04695 (x UInt64) ENGINE = Merge(currentDatabase(), '^v_04695$');
SELECT 'merge over view not null', modification_hash IS NOT NULL FROM system.tables WHERE database = currentDatabase() AND name = 'merge_over_view_04695';

CREATE TABLE dist_over_view_04695 AS v_04695 ENGINE = Distributed(test_shard_localhost, currentDatabase(), v_04695);
SELECT 'local distributed over view not null', modification_hash IS NOT NULL FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_view_04695';

-- A view whose stored SELECT reads an engine that cannot report changes still fails closed.
DROP TABLE IF EXISTS v_over_unsupported_04695;
CREATE VIEW v_over_unsupported_04695 AS SELECT name FROM system.tables;
SELECT 'view over unsupported null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 'v_over_unsupported_04695';

DROP TABLE v_over_unsupported_04695;
DROP TABLE dist_over_view_04695;
DROP TABLE merge_over_view_04695;
DROP TABLE hashes_04695;
DROP TABLE mv_04695;
DROP TABLE mv_src_04695;
DROP TABLE v_04695;
DROP TABLE t_04695;
