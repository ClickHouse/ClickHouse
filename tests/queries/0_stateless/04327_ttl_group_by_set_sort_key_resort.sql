-- Tags: no-random-merge-tree-settings
-- ^ The bug needs the TTL GROUP BY merge to actually run; pin MergeTree settings so the
--   tiny inputs are reliably merged into a single part.

-- Regression test for "TTL ... GROUP BY ... SET <col>" assigning a column the sorting key
-- depends on. The aggregation emits groups in the original (input) order and the part writer
-- takes the key columns out of that same block by name, in two shapes: the SET assigns a
-- sort-key column directly, so the data is physically unsorted (a debug build aborts with
-- "Sort order of blocks violated", a release build silently writes a corrupt part); or the SET
-- assigns a column that a sort-key EXPRESSION reads, where the expression column was
-- materialized before the TTL step, so the index describes a value the data no longer holds and
-- a key-range read misses rows. The fix recomputes the sorting key expressions from the
-- post-SET columns and re-sorts the output. Each check below confirms the merge succeeds AND
-- the resulting part is physically ordered by the sorting key.

-- Float64 sort key, non-monotonic SET on the first sort column. The last row is not expired, so
-- the aggregation also takes its flush-and-pass-through path: the aggregated groups and that row
-- are emitted interleaved, and the row's own k is smaller than both aggregates.
DROP TABLE IF EXISTS t_f64;
CREATE TABLE t_f64 (k Float64, ts DateTime, v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years, k = max(v)
SETTINGS min_bytes_for_full_part_storage = 128;
SYSTEM STOP MERGES t_f64;
INSERT INTO t_f64 VALUES (1.0, '2000-06-09 10:00', 96827);
INSERT INTO t_f64 VALUES (1.0, '2000-06-10 10:00', 41302), (1.0, '2100-01-01 10:00', 5);
SYSTEM START MERGES t_f64;
OPTIMIZE TABLE t_f64 FINAL;
SELECT 'f64 data', k, ts, v FROM t_f64 ORDER BY ALL;
-- The part must be physically ordered by the sorting key: the keys in physical row order must
-- already be sorted. Comparing against a second read with ORDER BY (k, toStartOfDay(ts)) would
-- prove nothing, because that read is answered from the part's own declared order and returns the
-- same rows even when the part is not sorted; arraySort does the ordering outside the planner.
SELECT 'f64 sorted', phys = arraySort(phys) FROM
    (SELECT groupArray((k, toStartOfDay(ts))) AS phys FROM (SELECT k, ts FROM t_f64 SETTINGS optimize_read_in_order = 0));
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
SELECT 'str sorted', phys = arraySort(phys) FROM
    (SELECT groupArray((id, toStartOfDay(ts))) AS phys FROM (SELECT id, ts FROM t_str SETTINGS optimize_read_in_order = 0));
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
SELECT 'lc sorted', phys = arraySort(phys) FROM
    (SELECT groupArray((k, toStartOfDay(ts))) AS phys FROM (SELECT k, ts FROM t_lc SETTINGS optimize_read_in_order = 0));
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
SELECT 'sub sorted', phys = arraySort(phys) FROM
    (SELECT groupArray((`t.a`, toStartOfDay(ts))) AS phys FROM (SELECT t.a, ts FROM t_sub SETTINGS optimize_read_in_order = 0));
DROP TABLE t_sub;

-- The sorting key only READS the column the SET assigns: ORDER BY (k, toStartOfDay(ts)) with
-- SET ts. toStartOfDay(ts) is materialized before the TTL step, so it keeps the pre-SET value and
-- stays ascending, which is why the part is written at all; the index then holds a day the data no
-- longer has. Here max(v) descends as the input day ascends, so the rewritten day descends too.
DROP TABLE IF EXISTS t_expr_key;
CREATE TABLE t_expr_key (k Float64, ts DateTime('UTC'), v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET ts = toDateTime('2100-01-01 00:00:00', 'UTC') + toIntervalDay(toUInt32(max(v)))
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         max_number_of_merges_with_ttl_in_pool = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
-- Group i (old day i) has max(v) = 39 - i. Two INSERTs, so OPTIMIZE FINAL always has work to do.
INSERT INTO t_expr_key SELECT 1.0,
    toDateTime('2000-01-01 10:00:00', 'UTC') + toIntervalDay(number DIV 2) + toIntervalHour(number % 2),
    39 - (number DIV 2) FROM numbers(40);
INSERT INTO t_expr_key SELECT 1.0,
    toDateTime('2000-01-01 10:00:00', 'UTC') + toIntervalDay(number DIV 2) + toIntervalHour(number % 2),
    39 - (number DIV 2) FROM numbers(40, 40);
OPTIMIZE TABLE t_expr_key FINAL SETTINGS optimize_throw_if_noop = 1;
-- With more than one active part, ORDER BY _part_offset interleaves them and the order oracle
-- below goes vacuous.
SELECT 'expr key parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_expr_key' AND active;
SELECT 'expr key rows', count() FROM t_expr_key;
SELECT 'expr key sorted', phys = arraySort(phys) FROM
    (SELECT groupArray(d) AS phys FROM (SELECT toStartOfDay(ts) AS d FROM t_expr_key ORDER BY _part_offset));
-- The stale index prunes granules that do hold matching rows: 40 groups over 10 granules, so the
-- granules this predicate needs are interior ones. Before the fix the read returned 0 of 9 rows.
SELECT 'expr key pruning', count() = (SELECT countIf(toStartOfDay(ts) >= toDateTime('2100-02-01 00:00:00', 'UTC')) FROM t_expr_key)
FROM t_expr_key WHERE toStartOfDay(ts) >= toDateTime('2100-02-01 00:00:00', 'UTC');
-- The merge that READS the part is where the violation surfaces: it recomputes toStartOfDay(ts)
-- from the stored ts. The added row is not expired, so this merge runs no aggregation of its own.
INSERT INTO t_expr_key VALUES (1.0, '2100-06-01 10:00:00', 1);
OPTIMIZE TABLE t_expr_key FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'expr key next merge rows', count() FROM t_expr_key;
DROP TABLE t_expr_key;

-- Same expression shape with a rewrite that is MONOTONE in the group's day: five expired days
-- each collapse into one row in 2100-06, and one row at 2050-01-01 does not expire and is passed
-- through last. An aggregated row jumping over a row that did not expire is enough on its own.
DROP TABLE IF EXISTS t_expr_key_live;
CREATE TABLE t_expr_key_live (k Float64, ts DateTime('UTC'), v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET ts = max(ts) + interval 100 years
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         max_number_of_merges_with_ttl_in_pool = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_expr_key_live SELECT 1.0,
    toDateTime('2000-06-01 10:00:00', 'UTC') + toIntervalDay(number DIV 2) + toIntervalHour(number % 2),
    number FROM numbers(10);
INSERT INTO t_expr_key_live VALUES (1.0, '2050-01-01 10:00:00', 999);
OPTIMIZE TABLE t_expr_key_live FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'expr key live parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_expr_key_live' AND active;
SELECT 'expr key live rows', count() FROM t_expr_key_live;
SELECT 'expr key live sorted', phys = arraySort(phys) FROM
    (SELECT groupArray(d) AS phys FROM (SELECT toStartOfDay(ts) AS d FROM t_expr_key_live ORDER BY _part_offset));
DROP TABLE t_expr_key_live;

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
SELECT 'mut sorted', phys = arraySort(phys) FROM
    (SELECT groupArray((k, toStartOfDay(ts))) AS phys FROM (SELECT k, ts FROM t_mut SETTINGS optimize_read_in_order = 0));
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
SELECT 'mut sub sorted', phys = arraySort(phys) FROM
    (SELECT groupArray((`t.a`, toStartOfDay(ts))) AS phys FROM (SELECT t.a, ts FROM t_mut_sub SETTINGS optimize_read_in_order = 0));
DROP TABLE t_mut_sub;

-- Mutation path, COMPUTED secondary index over a sort-key column the SET rewrites. A
-- `MATERIALIZE TTL` marks every column as changed while a GROUP BY TTL exists, so `idx (k + 1)`
-- is rebuilt from the stream. Its expression must be computed AFTER the TTL step and the
-- re-sort: computed before, it holds the pre-SET k and the index then prunes granules that do
-- hold matching rows. The index column IS in the sorting key here, which is what makes the
-- rebuilt index reachable through the repair's own shape.
DROP TABLE IF EXISTS t_mut_computed_idx;
CREATE TABLE t_mut_computed_idx (k UInt32, ts DateTime, v UInt32,
    INDEX idx (k + 1) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts) SET ts = max(ts) + interval 100 years, k = max(v)
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 1, index_granularity = 4,
         materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_mut_computed_idx;
-- Pre-SET k is number (0..39); after SET k = max(v) it becomes number + 100000.
INSERT INTO t_mut_computed_idx SELECT number, '2000-01-01 00:00:00', number + 100000 FROM numbers(40);
ALTER TABLE t_mut_computed_idx MATERIALIZE TTL SETTINGS mutations_sync = 2;
-- Three rewritten values from three different granules (rows 0, 20 and 39 at index_granularity 4)
-- must survive a read forced through the index; against a pre-SET index their granules hold
-- k + 1 in [1, 40] and every one of them is pruned instead. force_data_skipping_indices also
-- fails outright if the index was left unregistered. Note that a RANGE condition such as
-- k + 1 >= 100001 is answered from the primary key here (k is the first sort column), so it
-- returns every row whatever the skip index holds: it cannot stand in for this oracle.
SELECT 'computed idx present', count() FROM t_mut_computed_idx WHERE k + 1 IN (100001, 100021, 100040)
    SETTINGS force_data_skipping_indices = 'idx', use_skip_indexes = 1;
-- Control: the index must not change the result of that read.
SELECT 'computed idx matches', (SELECT count() FROM t_mut_computed_idx WHERE k + 1 IN (100001, 100021, 100040) SETTINGS use_skip_indexes = 1)
                             = (SELECT count() FROM t_mut_computed_idx WHERE k + 1 IN (100001, 100021, 100040) SETTINGS use_skip_indexes = 0);
DROP TABLE t_mut_computed_idx;

-- Mutation path, sort-key EXPRESSION shape. The mutation recomputes the expression and re-sorts
-- through its own implementation, separate from the merge one, so the shape needs an arm on both
-- paths. Same fixture as t_expr_key; the mutation always runs, so one INSERT is enough.
DROP TABLE IF EXISTS t_mut_expr_key;
CREATE TABLE t_mut_expr_key (k Float64, ts DateTime('UTC'), v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts)
    SET ts = toDateTime('2100-01-01 00:00:00', 'UTC') + toIntervalDay(toUInt32(max(v)))
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_mut_expr_key;
INSERT INTO t_mut_expr_key SELECT 1.0,
    toDateTime('2000-01-01 10:00:00', 'UTC') + toIntervalDay(number DIV 2) + toIntervalHour(number % 2),
    39 - (number DIV 2) FROM numbers(80);
ALTER TABLE t_mut_expr_key MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'mut expr key parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mut_expr_key' AND active;
SELECT 'mut expr key rows', count() FROM t_mut_expr_key;
SELECT 'mut expr key sorted', phys = arraySort(phys) FROM
    (SELECT groupArray(d) AS phys FROM (SELECT toStartOfDay(ts) AS d FROM t_mut_expr_key ORDER BY _part_offset));
DROP TABLE t_mut_expr_key;

-- Several GROUP BY TTLs in one part: no repair runs at all, because an earlier SET can rewrite a
-- column a later TTL groups by and the re-sort would only hide the resulting wrong groups behind
-- a correctly ordered part. Observed through the external-sort counter: the re-sort is bounded at
-- one byte, so it must spill whenever it runs, and zero spilled parts means it did not. The SET
-- assigns `ts`, which the sorting key reads through toStartOfDay(ts): the pre-materialized
-- expression column then goes stale but stays ascending, so the part is still written and the
-- probe is what tells the two shapes apart. The single-clause control is the same fixture with
-- one TTL, where the repair must run.
DROP TABLE IF EXISTS t_multi_ttl;
CREATE TABLE t_multi_ttl (k UInt32, ts DateTime, v UInt32)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts) SET ts = max(ts) + interval 100 years,
    ts + toIntervalDay(2) GROUP BY k SET v = max(v)
SETTINGS min_bytes_for_wide_part = 0, ttl_resort_max_bytes_before_external_sort = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1, max_number_of_merges_with_ttl_in_pool = 0;
DROP TABLE IF EXISTS t_single_ttl;
CREATE TABLE t_single_ttl (k UInt32, ts DateTime, v UInt32)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts) SET ts = max(ts) + interval 100 years
SETTINGS min_bytes_for_wide_part = 0, ttl_resort_max_bytes_before_external_sort = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1, max_number_of_merges_with_ttl_in_pool = 0;
INSERT INTO t_multi_ttl SELECT number % 100, toDateTime('2000-06-09 10:00:00') + (number % 5) * 86400, number FROM numbers(1000);
INSERT INTO t_multi_ttl SELECT number % 100, toDateTime('2000-06-09 10:00:00') + (number % 5) * 86400, number FROM numbers(1000, 1000);
INSERT INTO t_single_ttl SELECT number % 100, toDateTime('2000-06-09 10:00:00') + (number % 5) * 86400, number FROM numbers(1000);
INSERT INTO t_single_ttl SELECT number % 100, toDateTime('2000-06-09 10:00:00') + (number % 5) * 86400, number FROM numbers(1000, 1000);
-- optimize_throw_if_noop turns a merge that was not assigned into an error, so the probes below
-- cannot read pre-merge parts of a table OPTIMIZE reported success on but left untouched.
OPTIMIZE TABLE t_multi_ttl FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE t_single_ttl FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'multi ttl rows', count() FROM t_multi_ttl;
SELECT 'single ttl rows', count() FROM t_single_ttl;
SYSTEM FLUSH LOGS part_log;
SELECT 'multi ttl not repaired', sum(ProfileEvents['ExternalSortWritePart']) = 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 't_multi_ttl' AND event_type = 'MergeParts';
SELECT 'single ttl repaired', sum(ProfileEvents['ExternalSortWritePart']) > 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 't_single_ttl' AND event_type = 'MergeParts';
DROP TABLE t_multi_ttl;
DROP TABLE t_single_ttl;

-- An unrelated expired TTL must not trigger the repair when the GROUP BY ... SET TTL itself does
-- not fire: the DELETE TTL is expired, while the GROUP BY toStartOfDay(ts) SET ts clause (the only
-- one touching the sort key) expires 40 years out. The DELETE result must be correct and the
-- re-sort must not have run, again read from the external-sort counter.
DROP TABLE IF EXISTS t_not_firing;
CREATE TABLE t_not_firing (ts DateTime, payload UInt64)
ENGINE = MergeTree ORDER BY toStartOfDay(ts)
TTL ts + toIntervalDay(1) DELETE WHERE payload < 5,
    ts + toIntervalYear(40) GROUP BY toStartOfDay(ts) SET ts = max(ts), payload = sum(payload)
SETTINGS min_bytes_for_wide_part = 0, ttl_resort_max_bytes_before_external_sort = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1, max_number_of_merges_with_ttl_in_pool = 0;
INSERT INTO t_not_firing SELECT toDateTime('2020-01-01 00:00:00') + toIntervalDay(number % 3), number FROM numbers(30);
INSERT INTO t_not_firing SELECT toDateTime('2020-01-01 00:00:00') + toIntervalDay(number % 3), number FROM numbers(30, 30);
OPTIMIZE TABLE t_not_firing FINAL SETTINGS optimize_throw_if_noop = 1;
-- Rows with payload < 5 are deleted, nothing is aggregated.
SELECT 'not firing rows', count(), min(payload), sum(payload) FROM t_not_firing;
SYSTEM FLUSH LOGS part_log;
SELECT 'not firing not repaired', sum(ProfileEvents['ExternalSortWritePart']) = 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 't_not_firing' AND event_type = 'MergeParts';
DROP TABLE t_not_firing;

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
SELECT 'nonkey sorted', phys = arraySort(phys) FROM
    (SELECT groupArray((k, toStartOfDay(ts))) AS phys FROM (SELECT k, ts FROM t_nonkey SETTINGS optimize_read_in_order = 0));
DROP TABLE t_nonkey;
