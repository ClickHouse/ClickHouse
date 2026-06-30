-- Tags: no-random-merge-tree-settings
-- ^ The bug needs the TTL GROUP BY merge to actually run; pin MergeTree settings so the
--   tiny inputs are reliably merged into a single part.

-- Regression test for "TTL ... GROUP BY ... SET <col>" assigning a column the sorting key
-- depends on. The aggregation emits groups in the original (input) order, so when SET
-- rewrites a sort-key column the produced stream is no longer ordered by the sorting key.
-- Before the fix this wrote a part with a primary index inconsistent with the data: a debug
-- build aborts with "Sort order of blocks violated", a release build silently writes a
-- corrupt part. The fix recomputes the sorting key after the SET and re-sorts the merge
-- output. Each check below confirms the merge succeeds AND the resulting part is physically
-- ordered by the sorting key (the natural read order equals the ORDER BY read order).

-- Float64 sort key, non-monotonic SET on the first sort column.
DROP TABLE IF EXISTS t_f64;
CREATE TABLE t_f64 (k Float64, ts DateTime, v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years, k = max(v)
SETTINGS min_bytes_for_full_part_storage = 128;
SYSTEM STOP MERGES t_f64;
INSERT INTO t_f64 VALUES (1.0, '2000-06-09 10:00', 96827);
INSERT INTO t_f64 VALUES (1.0, '2000-06-10 10:00', 41302);
SYSTEM START MERGES t_f64;
OPTIMIZE TABLE t_f64 FINAL;
SELECT 'f64 data', k, ts, v FROM t_f64 ORDER BY ALL;
-- Part must be physically sorted: natural read order equals ORDER BY read order.
SELECT 'f64 sorted', (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_f64 SETTINGS optimize_read_in_order = 0))
                   = (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_f64 ORDER BY k, toStartOfDay(ts)));
DROP TABLE t_f64;

-- String sort key, SET on the first sort column.
DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (id String, ts DateTime, value String)
ENGINE = MergeTree ORDER BY (id, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY id, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years, id = max(value)
SETTINGS min_bytes_for_full_part_storage = 128;
SYSTEM STOP MERGES t_str;
INSERT INTO t_str VALUES ('p', '2000-06-09 10:00', 'zzz');
INSERT INTO t_str VALUES ('p', '2000-06-10 10:00', 'aaa');
SYSTEM START MERGES t_str;
OPTIMIZE TABLE t_str FINAL;
SELECT 'str data', id, ts, value FROM t_str ORDER BY ALL;
SELECT 'str sorted', (SELECT groupArray((id, toStartOfDay(ts))) FROM (SELECT id, ts FROM t_str SETTINGS optimize_read_in_order = 0))
                   = (SELECT groupArray((id, toStartOfDay(ts))) FROM (SELECT id, ts FROM t_str ORDER BY id, toStartOfDay(ts)));
DROP TABLE t_str;

-- LowCardinality(String) sort key, SET on the first sort column.
DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (k LowCardinality(String), ts DateTime, cand LowCardinality(String), v UInt32)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years, k = argMax(cand, v)
SETTINGS min_bytes_for_full_part_storage = 128;
SYSTEM STOP MERGES t_lc;
INSERT INTO t_lc VALUES ('a', '2000-06-09 10:00', 'zzz', 500);
INSERT INTO t_lc VALUES ('a', '2000-06-10 10:00', 'aaa', 100);
SYSTEM START MERGES t_lc;
OPTIMIZE TABLE t_lc FINAL;
SELECT 'lc data', k, ts FROM t_lc ORDER BY ALL;
SELECT 'lc sorted', (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_lc SETTINGS optimize_read_in_order = 0))
                  = (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_lc ORDER BY k, toStartOfDay(ts)));
DROP TABLE t_lc;

-- Subcolumn sort key: ORDER BY references the subcolumn t.a, while the SET assigns the whole
-- physical column t. The re-sort gate must map the sorting-key dependency t.a to its storage
-- column t before comparing it with the SET target, otherwise the stale materialized t.a (from
-- the pre-merge sort) survives and the part is built from the pre-SET value.
DROP TABLE IF EXISTS t_sub;
CREATE TABLE t_sub (t Tuple(a UInt32, b UInt32), ts DateTime, cand Tuple(a UInt32, b UInt32), v UInt32)
ENGINE = MergeTree ORDER BY (t.a, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY t.a, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years, t = argMax(cand, v)
SETTINGS min_bytes_for_full_part_storage = 128;
SYSTEM STOP MERGES t_sub;
INSERT INTO t_sub VALUES ((5, 0), '2000-06-09 10:00', (900, 0), 10);
INSERT INTO t_sub VALUES ((5, 0), '2000-06-10 10:00', (100, 0), 20);
SYSTEM START MERGES t_sub;
OPTIMIZE TABLE t_sub FINAL;
SELECT 'sub data', t.a, ts FROM t_sub ORDER BY ALL;
SELECT 'sub sorted', (SELECT groupArray((t.a, toStartOfDay(ts))) FROM (SELECT t.a, ts FROM t_sub SETTINGS optimize_read_in_order = 0))
                   = (SELECT groupArray((t.a, toStartOfDay(ts))) FROM (SELECT t.a, ts FROM t_sub ORDER BY t.a, toStartOfDay(ts)));
DROP TABLE t_sub;

-- Mutation path: the same violation is reachable through ALTER TABLE ... MATERIALIZE TTL.
-- The mutation runs the GROUP BY ... SET aggregation through the mutation pipeline and the
-- full-rewrite writer also rebuilds the primary index from the stream, so the post-SET stream
-- must be re-sorted there too (the merge fix only covers the merge pipeline). STOP TTL MERGES
-- keeps the background TTL merge from applying the TTL first, so the mutation is the only path
-- that runs it. Plain Float64 and the subcolumn case (which exercises the storage-name mapping).
DROP TABLE IF EXISTS t_mut;
CREATE TABLE t_mut (k Float64, ts DateTime, v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years, k = max(v)
SETTINGS min_bytes_for_full_part_storage = 128, materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_mut;
INSERT INTO t_mut VALUES (1.0, '2000-06-09 10:00', 96827), (1.0, '2000-06-10 10:00', 41302);
ALTER TABLE t_mut MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'mut data', k, ts, v FROM t_mut ORDER BY ALL;
SELECT 'mut sorted', (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_mut SETTINGS optimize_read_in_order = 0))
                   = (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_mut ORDER BY k, toStartOfDay(ts)));
DROP TABLE t_mut;

DROP TABLE IF EXISTS t_mut_sub;
CREATE TABLE t_mut_sub (t Tuple(a UInt32, b UInt32), ts DateTime, cand Tuple(a UInt32, b UInt32), v UInt32)
ENGINE = MergeTree ORDER BY (t.a, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY t.a, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years, t = argMax(cand, v)
SETTINGS min_bytes_for_full_part_storage = 128, materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_mut_sub;
INSERT INTO t_mut_sub VALUES ((5, 0), '2000-06-09 10:00', (900, 0), 10), ((5, 0), '2000-06-10 10:00', (100, 0), 20);
ALTER TABLE t_mut_sub MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'mut sub data', t.a, ts FROM t_mut_sub ORDER BY ALL;
SELECT 'mut sub sorted', (SELECT groupArray((t.a, toStartOfDay(ts))) FROM (SELECT t.a, ts FROM t_mut_sub SETTINGS optimize_read_in_order = 0))
                       = (SELECT groupArray((t.a, toStartOfDay(ts))) FROM (SELECT t.a, ts FROM t_mut_sub ORDER BY t.a, toStartOfDay(ts)));
DROP TABLE t_mut_sub;

-- Mutation path, secondary index on a subcolumn of a TTL-rewritten column. A `MATERIALIZE TTL`
-- with a GROUP BY TTL records the rewritten physical column `t` as the changed column, while the
-- skip index depends on the subcolumn `t.a`. The rebuild decision must map `t.a` to its storage
-- column `t`, otherwise the index is hardlinked from the source part and keeps pre-SET minmax
-- values; the index expression must also be recomputed after the TTL SET (computing it before the
-- aggregation crashes the mutation). The index column `t.a` is intentionally NOT in the sorting
-- key so the primary key cannot mask a stale skip index during pruning.
DROP TABLE IF EXISTS t_mut_idx;
CREATE TABLE t_mut_idx (sk UInt32, t Tuple(a UInt32, b UInt32), ts DateTime, cand Tuple(a UInt32, b UInt32), v UInt32,
    INDEX idx t.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY sk
TTL ts + toIntervalDay(1) GROUP BY sk SET ts = max(ts) + interval 100 years, t = argMax(cand, v)
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 1, index_granularity = 4, materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_mut_idx;
-- Pre-SET t.a is number (0..39); after SET t = argMax(cand, v) it becomes number + 100000.
INSERT INTO t_mut_idx SELECT number, (number, 0), '2000-01-01 00:00:00', (number + 100000, 0), 1 FROM numbers(40);
ALTER TABLE t_mut_idx MATERIALIZE TTL SETTINGS mutations_sync = 2;
-- The skip index must reflect the post-SET values. Force the query to use the index
-- (force_data_skipping_indices) and select the rewritten values: all 40 rows must be returned. A
-- stale (pre-SET) index holds the old `t.a` minmax ranges and would prune the rewritten values
-- away, returning fewer rows. force_data_skipping_indices also fails outright if the index was left
-- unregistered. (EXPLAIN granule counts are avoided here as they depend on randomized settings.)
SELECT 'mut idx present', count() FROM t_mut_idx WHERE t.a >= 100000 SETTINGS force_data_skipping_indices = 'idx', use_skip_indexes = 1;
-- Result with the index must match the result without it for every rewritten value.
SELECT 'mut idx matches', (SELECT count() FROM t_mut_idx WHERE t.a IN (100000, 100020, 100039) SETTINGS use_skip_indexes = 1)
                        = (SELECT count() FROM t_mut_idx WHERE t.a IN (100000, 100020, 100039) SETTINGS use_skip_indexes = 0);
DROP TABLE t_mut_idx;

-- Control: SET only a non-sort-key column. The re-sort must not be needed and the merge
-- must work exactly as before.
DROP TABLE IF EXISTS t_nonkey;
CREATE TABLE t_nonkey (k UInt32, ts DateTime, v UInt32)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET v = max(v)
SETTINGS min_bytes_for_full_part_storage = 128;
SYSTEM STOP MERGES t_nonkey;
INSERT INTO t_nonkey VALUES (5, '2000-06-09 10:00', 100);
INSERT INTO t_nonkey VALUES (3, '2000-06-10 10:00', 200);
SYSTEM START MERGES t_nonkey;
OPTIMIZE TABLE t_nonkey FINAL;
SELECT 'nonkey data', k, ts, v FROM t_nonkey ORDER BY ALL;
SELECT 'nonkey sorted', (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_nonkey SETTINGS optimize_read_in_order = 0))
                      = (SELECT groupArray((k, toStartOfDay(ts))) FROM (SELECT k, ts FROM t_nonkey ORDER BY k, toStartOfDay(ts)));
DROP TABLE t_nonkey;
