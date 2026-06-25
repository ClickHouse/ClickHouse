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
