-- Tags: no-random-merge-tree-settings
-- ^ The bug needs the TTL GROUP BY merge to actually run; pin MergeTree settings so the
--   tiny inputs are reliably merged into a single part.

-- Non-firing / const-folded / unsorted-aggregation half of the "TTL ... GROUP BY ... SET on a
-- sorting key column" regression test. Split out of 04327_ttl_group_by_set_sort_key_resort so
-- that neither file runs into the per-test time limit in the slower CI configurations (S3
-- storage with metadata in Keeper), where the per-DDL round trips dominate. Nothing is dropped.

-- A future GROUP BY TTL does not fire during MATERIALIZE TTL. Its SET target must therefore not
-- trigger the post-TTL MATERIALIZED-column repair: `m` contains `now()` and would change despite
-- unchanged source data if the repair were keyed on table metadata instead of firing TTL targets.
DROP TABLE IF EXISTS t_future_ttl_mut;
CREATE TABLE t_future_ttl_mut
(
    ts DateTime,
    x UInt32,
    m String MATERIALIZED concat(toString(x), '|', toString(now())),
    saved_m String DEFAULT m
)
ENGINE = MergeTree ORDER BY x
TTL ts + toIntervalYear(50) GROUP BY x SET x = max(x) + 1
SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_future_ttl_mut;
INSERT INTO t_future_ttl_mut (ts, x) VALUES ('2020-01-15 00:00:00', 7), ('2020-02-15 00:00:00', 8);
ALTER TABLE t_future_ttl_mut MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'future ttl leaves materialized', countIf(m = saved_m) = count() FROM t_future_ttl_mut;
DROP TABLE t_future_ttl_mut;

-- A part written before a future GROUP BY TTL is added has no corresponding ttl.txt entry.
-- Materializing that new TTL must not recompute a non-deterministic MATERIALIZED default when
-- the TTL keeps every row unchanged.
DROP TABLE IF EXISTS t_future_ttl_after_modify;
CREATE TABLE t_future_ttl_after_modify
(
    ts DateTime,
    x UInt32,
    m String MATERIALIZED concat(toString(x), '|', toString(generateUUIDv4()))
)
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_future_ttl_after_modify;
INSERT INTO t_future_ttl_after_modify (ts, x) VALUES ('2020-01-15 00:00:00', 7), ('2020-02-15 00:00:00', 8);
CREATE TABLE t_future_ttl_after_modify_saved ENGINE = Memory AS SELECT x, m FROM t_future_ttl_after_modify;
SET materialize_ttl_after_modify = 0;
ALTER TABLE t_future_ttl_after_modify MODIFY TTL ts + toIntervalYear(30) GROUP BY x SET x = max(x) + 1;
SET materialize_ttl_after_modify = 1;
ALTER TABLE t_future_ttl_after_modify MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'future ttl after modify leaves materialized', countIf(t.m = s.m) = count() FROM t_future_ttl_after_modify AS t INNER JOIN t_future_ttl_after_modify_saved AS s USING x;
DROP TABLE t_future_ttl_after_modify_saved;
DROP TABLE t_future_ttl_after_modify;

-- `force_` in `MATERIALIZE TTL` evaluates every TTL expression, but it does not make a future
-- GROUP BY TTL fire. In particular, its SET targets must not refresh a MATERIALIZED expiry input
-- of a later column TTL: `m` contains `now()` and would visibly change even though `x` is unchanged.
DROP TABLE IF EXISTS t_future_group_by_before_column_ttl;
CREATE TABLE t_future_group_by_before_column_ttl
(
    ts DateTime,
    x UInt32,
    m DateTime MATERIALIZED now() + toIntervalSecond(x),
    saved_m DateTime DEFAULT m,
    payload UInt32 DEFAULT 1 TTL m + toIntervalDay(1)
)
ENGINE = MergeTree ORDER BY x
TTL ts + toIntervalYear(50) GROUP BY x SET x = max(x) + 1
SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_future_group_by_before_column_ttl;
INSERT INTO t_future_group_by_before_column_ttl (ts, x) VALUES ('2020-01-15 00:00:00', 7), ('2020-02-15 00:00:00', 8);
ALTER TABLE t_future_group_by_before_column_ttl MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT 'future group by leaves column ttl materialized', countIf(m = saved_m) = count() FROM t_future_group_by_before_column_ttl;
DROP TABLE t_future_group_by_before_column_ttl;

-- A recomputed MATERIALIZED column whose expression constant-folds (`isNull` of a non-Nullable
-- column) yields a ColumnConst, which the part writer cannot serialize. `m` is seeded stale so the
-- assertions below fail if the recompute silently declines instead of running.
DROP TABLE IF EXISTS t_const_folded_mat;
CREATE TABLE t_const_folded_mat (k UInt32, x DateTime, m UInt8 MATERIALIZED isNull(x))
ENGINE = MergeTree ORDER BY k
TTL x + toIntervalDay(1) GROUP BY k SET x = max(x) + toIntervalYear(30)
SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP TTL MERGES t_const_folded_mat;
INSERT INTO t_const_folded_mat (k, x, m)
SETTINGS insert_allow_materialized_columns = 1
VALUES (1, '2020-01-01 00:00:00', 7), (2, '2020-01-02 00:00:00', 9);
SELECT 'const folded mat seeded', countIf(m = 7) + countIf(m = 9) FROM t_const_folded_mat;
SYSTEM START TTL MERGES t_const_folded_mat;
OPTIMIZE TABLE t_const_folded_mat FINAL;
-- The SET fired (both rows expired, one group per key), so `m` must be recomputed to 0.
SELECT 'const folded mat set fired', countIf(toYear(x) = 2050) FROM t_const_folded_mat;
SELECT 'const folded mat recomputed', count(), countIf(m = 0) FROM t_const_folded_mat;
DROP TABLE t_const_folded_mat;

-- A SET expression whose arguments are all constant is const-folded, so the result arrives
-- as a ColumnConst while the destination is a ColumnTuple.
CREATE TABLE t_const_folded_tuple (k UInt32, ts DateTime, tup Tuple(ts DateTime))
ENGINE = MergeTree ORDER BY k
TTL ts + toIntervalDay(1) GROUP BY k
SET tup = tuple(defaultValueOfArgumentType(toDateTime('2200-01-01') + toIntervalSecond(max(k))))
SETTINGS remove_empty_parts = 0;
INSERT INTO t_const_folded_tuple VALUES (1, '2020-01-01 00:00:00', ('2020-01-01 00:00:00')), (1, '2020-01-02 00:00:00', ('2020-01-02 00:00:00'));
OPTIMIZE TABLE t_const_folded_tuple FINAL;
SELECT 'const folded tuple rows', count() FROM t_const_folded_tuple;
SELECT 'const folded tuple value', tup.ts = toDateTime(0) FROM t_const_folded_tuple;
DROP TABLE t_const_folded_tuple;

-- A `SET` in an earlier GROUP BY TTL rewrites the grouping key of the later TTL, so the latter
-- must use its unsorted hash-aggregation path. Its dedicated table setting bounds that path,
-- independently of the global ratio gate: with a one-byte limit it must spill rather than retain
-- the whole part in memory. The `SET a = max(b)` maps key a to 100 - a, a bijection, so the later
-- TTL still sees 100 distinct groups and the row count is preserved.
DROP TABLE IF EXISTS t_unsorted_group_by_spill;
CREATE TABLE t_unsorted_group_by_spill (a UInt32, b UInt32, ts DateTime)
ENGINE = MergeTree ORDER BY a
TTL ts + toIntervalDay(1) GROUP BY a SET a = max(b),
    ts + toIntervalDay(2) GROUP BY a SET b = max(b)
SETTINGS min_bytes_for_full_part_storage = 128, ttl_group_by_unsorted_max_bytes_before_external_group_by = 1;
SYSTEM STOP MERGES t_unsorted_group_by_spill;
INSERT INTO t_unsorted_group_by_spill
    SELECT number % 100, 100 - number % 100, toDateTime('2000-01-01') FROM numbers(50000);
SYSTEM START MERGES t_unsorted_group_by_spill;
OPTIMIZE TABLE t_unsorted_group_by_spill FINAL;
SELECT 'unsorted group by count', count() FROM t_unsorted_group_by_spill;
SYSTEM FLUSH LOGS part_log;
SELECT 'unsorted group by spilled', max(ProfileEvents['ExternalAggregationWritePart']) > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_unsorted_group_by_spill' AND event_type = 'MergeParts' AND error = 0;
DROP TABLE t_unsorted_group_by_spill;
