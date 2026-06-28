-- A materialized view created `AS SELECT ... SETTINGS <construction setting>` keeps the setting verbatim
-- in its stored definition (so `SHOW CREATE` shows it) and materializes it on the inner query that
-- `InsertDependenciesBuilder` runs when source rows arrive — so the rows pushed to the target table are
-- shaped (filtered / limited), the same way a regular view's read is shaped. This covers the stored
-- materialized-view gap: the inner query bypasses `executeQuery`'s wrapping, so it is materialized in the
-- `StorageMaterializedView` constructor instead (mirroring the `StorageView` path).

DROP TABLE IF EXISTS t_src_filter;
DROP TABLE IF EXISTS t_dst_filter;
DROP TABLE IF EXISTS t_src_limit;
DROP TABLE IF EXISTS t_dst_limit;
DROP VIEW IF EXISTS mv_filter;
DROP VIEW IF EXISTS mv_limit;

SELECT '-- `filter` in the MV definition filters the rows pushed to the target (only x >= 7)';
CREATE TABLE t_src_filter (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_dst_filter (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_filter TO t_dst_filter AS SELECT x FROM t_src_filter SETTINGS filter = 'x >= 7';
INSERT INTO t_src_filter SELECT number FROM numbers(10);
SELECT x FROM t_dst_filter ORDER BY x;

SELECT '-- the construction setting is preserved verbatim in SHOW CREATE';
SHOW CREATE TABLE mv_filter FORMAT TSVRaw;

SELECT '-- `limit` in the MV definition caps the rows materialized from a single inserted block (3 of 10)';
CREATE TABLE t_src_limit (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_dst_limit (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_limit TO t_dst_limit AS SELECT x FROM t_src_limit ORDER BY x SETTINGS limit = 3;
INSERT INTO t_src_limit SELECT number FROM numbers(10) SETTINGS max_block_size = 65536, max_insert_block_size = 65536;
SELECT count() FROM t_dst_limit;

DROP VIEW mv_filter;
DROP VIEW mv_limit;
DROP TABLE t_dst_filter;
DROP TABLE t_dst_limit;
DROP TABLE t_src_filter;
DROP TABLE t_src_limit;
