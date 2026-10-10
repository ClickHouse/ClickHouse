-- A projection stores values that were computed from the table's columns as they were when the
-- projection part was written: the grouping keys of an aggregate projection, and the sort order of a
-- normal projection. Changing the type of a column such a stored value was computed from does not
-- rewrite the part, so a query answered from the projection can return values the table no longer
-- holds. This test asserts that reading a query with projections enabled returns exactly what the
-- same query returns with projections disabled, in every way a column's type can get ahead of what a
-- part records: a conversion that is still pending, a part cloned from another table, a type change
-- that rewrites nothing at all, and a column the part holds no values of.

-- Read-in-order on the base table declines a forced projection, and the plan shape is not the
-- subject here.
SET optimize_read_in_order = 0;

SELECT '-- A. changing only a time zone rewrites no data, so nothing can rebuild the projection';
DROP TABLE IF EXISTS t_retype_tz;
CREATE TABLE t_retype_tz
(
    id UInt64,
    dt DateTime('UTC'),
    PROJECTION p (SELECT toHour(dt), count() GROUP BY toHour(dt))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
INSERT INTO t_retype_tz SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(8);
ALTER TABLE t_retype_tz MODIFY COLUMN dt DateTime('Asia/Tokyo') SETTINGS mutations_sync = 2, alter_sync = 2;
-- there is no mutation to wait for and none that could ever refresh the stored hours
SELECT 'A mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_retype_tz';
SELECT 'A authoritative', toHour(dt) AS h, count() AS c FROM t_retype_tz GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 0;
SELECT 'A with projections', toHour(dt) AS h, count() AS c FROM t_retype_tz GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 1;
DROP TABLE t_retype_tz;

SELECT '-- B. a type conversion that has not been applied yet';
DROP TABLE IF EXISTS t_retype_pending;
CREATE TABLE t_retype_pending
(
    id UInt64,
    b Int32,
    PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
-- above 2^24 the values do not survive Float32, so the conversion changes what toInt64 returns
INSERT INTO t_retype_pending SELECT number, toInt32(1073741825 + number * 2) FROM numbers(4);
SYSTEM STOP MERGES t_retype_pending;
ALTER TABLE t_retype_pending MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'B pending', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_retype_pending' AND NOT is_done;
SELECT 'B authoritative', toInt64(b) AS k, count() AS c FROM t_retype_pending GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'B with projections', toInt64(b) AS k, count() AS c FROM t_retype_pending GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;

SELECT '-- G. asking for the projection explicitly now reports that it cannot be used';
SELECT toInt64(b) AS k, count() AS c FROM t_retype_pending GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

SELECT '-- E. a part written after the type change keeps its projection, and the mixed read is correct';
INSERT INTO t_retype_pending SELECT 100 + number, toInt32(7) FROM numbers(2);
SELECT 'E authoritative', toInt64(b) AS k, count() AS c FROM t_retype_pending GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'E with projections', toInt64(b) AS k, count() AS c FROM t_retype_pending GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
DROP TABLE t_retype_pending;

SELECT '-- C. a partition moved into another table, which has no conversion left to apply';
DROP TABLE IF EXISTS t_retype_clone_src;
DROP TABLE IF EXISTS t_retype_clone_dst;
CREATE TABLE t_retype_clone_src
(
    id UInt64,
    b Int32,
    p UInt8,
    PROJECTION p_e (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
INSERT INTO t_retype_clone_src SELECT number, toInt32(1073741825 + number * 2), 1 FROM numbers(4);
SYSTEM STOP MERGES t_retype_clone_src;
ALTER TABLE t_retype_clone_src MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
CREATE TABLE t_retype_clone_dst
(
    id UInt64,
    b Float32,
    p UInt8,
    PROJECTION p_e (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_retype_clone_dst;
ALTER TABLE t_retype_clone_dst ATTACH PARTITION 1 FROM t_retype_clone_src;
SELECT 'C mutations at destination', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_retype_clone_dst';
SELECT 'C authoritative', toInt64(b) AS k, count() AS c FROM t_retype_clone_dst GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'C with projections', toInt64(b) AS k, count() AS c FROM t_retype_clone_dst GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
DROP TABLE t_retype_clone_src;
DROP TABLE t_retype_clone_dst;

SELECT '-- D. only the projection that reads the changed column is affected';
DROP TABLE IF EXISTS t_retype_scoped;
CREATE TABLE t_retype_scoped
(
    id UInt64,
    b Int32,
    other Int32,
    PROJECTION p_stale (SELECT toInt64(b), count() GROUP BY toInt64(b)),
    PROJECTION p_ok (SELECT toInt64(other), count() GROUP BY toInt64(other))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
INSERT INTO t_retype_scoped SELECT number, toInt32(1073741825 + number * 2), toInt32(number) FROM numbers(4);
SYSTEM STOP MERGES t_retype_scoped;
ALTER TABLE t_retype_scoped MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'D untouched projection', toInt64(other) AS k, count() AS c FROM t_retype_scoped GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p_ok';
SELECT 'D changed column authoritative', toInt64(b) AS k, count() AS c FROM t_retype_scoped GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'D changed column with projections', toInt64(b) AS k, count() AS c FROM t_retype_scoped GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
DROP TABLE t_retype_scoped;

SELECT '-- I. a normal projection sorted by an expression: its stored order can skip matching rows';
DROP TABLE IF EXISTS t_retype_normal;
CREATE TABLE t_retype_normal
(
    id UInt64,
    b Int32,
    PROJECTION p (SELECT id, b ORDER BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
INSERT INTO t_retype_normal SELECT number, toInt32(1073741825 + number * 2) FROM numbers(4);
SYSTEM STOP MERGES t_retype_normal;
ALTER TABLE t_retype_normal MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
-- after the conversion every row has the same value, which the stored order does not contain
SELECT 'I authoritative', count() FROM t_retype_normal WHERE toInt64(b) = 1073741824
SETTINGS optimize_use_projections = 0, use_query_condition_cache = 0, optimize_trivial_count_query = 0;
SELECT 'I with projections', count() FROM t_retype_normal WHERE toInt64(b) = 1073741824
SETTINGS optimize_use_projections = 1, use_query_condition_cache = 0, optimize_trivial_count_query = 0;
DROP TABLE t_retype_normal;

SELECT '-- I2. a normal projection that cannot answer the query and is used only to prune parts: its';
SELECT '--     stored order prunes away the very parts that hold the matching rows';
DROP TABLE IF EXISTS t_retype_filter_only;
CREATE TABLE t_retype_filter_only
(
    id UInt64,
    b Int32,
    payload String,
    PROJECTION p (SELECT id, b ORDER BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
-- one part whose values change under the conversion and one whose values survive it
INSERT INTO t_retype_filter_only SELECT number, toInt32(1073741825 + number * 2), 'big' FROM numbers(4);
INSERT INTO t_retype_filter_only SELECT 100 + number, toInt32(7 + number * 2), 'small' FROM numbers(2);
SYSTEM STOP MERGES t_retype_filter_only;
-- `payload` is not stored by the projection, so the projection can only prune parts. Automatic
-- column statistics prune the same part earlier, which would keep the projection out of the plan.
SELECT 'I2 control, projection prunes a part', count() FROM
(
    EXPLAIN projections = 1 SELECT payload FROM t_retype_filter_only WHERE toInt64(b) = 1073741825
    SETTINGS optimize_use_projections = 1, optimize_use_projection_filtering = 1,
             use_statistics_for_part_pruning = 0
)
WHERE explain ILIKE '%part-level filtering%';
ALTER TABLE t_retype_filter_only MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'I2 after the retype, projection prunes nothing', count() FROM
(
    EXPLAIN projections = 1 SELECT payload FROM t_retype_filter_only WHERE toInt64(b) = 1073741824
    SETTINGS optimize_use_projections = 1, optimize_use_projection_filtering = 1,
             use_statistics_for_part_pruning = 0
)
WHERE explain ILIKE '%part-level filtering%';
SELECT 'I2 authoritative', count() FROM t_retype_filter_only WHERE toInt64(b) = 1073741824
SETTINGS optimize_use_projections = 0, use_statistics_for_part_pruning = 0,
         use_query_condition_cache = 0, optimize_trivial_count_query = 0;
SELECT 'I2 with projections', count() FROM t_retype_filter_only WHERE toInt64(b) = 1073741824
SETTINGS optimize_use_projections = 1, optimize_use_projection_filtering = 1,
         use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
         optimize_trivial_count_query = 0;
DROP TABLE t_retype_filter_only;

SELECT '-- O. a normal projection storing a column the part does not hold: its value was computed';
SELECT '--    from the retyped column through the DEFAULT expression of that column';
DROP TABLE IF EXISTS t_retype_default_dep;
CREATE TABLE t_retype_default_dep (id UInt64, b Int32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_retype_default_dep SELECT number, toInt32(1073741825 + number * 2) FROM numbers(4);
-- metadata-only add, so the part never stores c; both read paths have to synthesise it
ALTER TABLE t_retype_default_dep ADD COLUMN c Int64 DEFAULT toInt64(b) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_default_dep ADD PROJECTION p (SELECT id, c ORDER BY id) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_default_dep MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'O c not stored by the part', count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_retype_default_dep' AND active AND column = 'c';
SELECT 'O c stored by the projection', count() FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_retype_default_dep' AND active AND name = 'p' AND column = 'c';
-- positive control BEFORE the retype: the projection answers with the values computed from Int32 b,
-- so the assertions below cannot pass by the projection never having held a value
SELECT 'O control, projection answers before the retype', c, count() AS n FROM t_retype_default_dep
GROUP BY c ORDER BY c SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SYSTEM STOP MERGES t_retype_default_dep;
ALTER TABLE t_retype_default_dep MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'O authoritative', c, count() AS n FROM t_retype_default_dep GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 0;
SELECT 'O with projections', c, count() AS n FROM t_retype_default_dep GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 1;
-- At default settings a normal projection is only considered when the query has a filter or an outer
-- ORDER BY its own order can serve (optimizeUseNormalProjections), which this GROUP BY has neither of,
-- so the stored value is observable here only when the projection is asked for explicitly.
SELECT c, count() AS n FROM t_retype_default_dep GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
DROP TABLE t_retype_default_dep;

SELECT '-- P. the DEFAULT of a stored column reads the retyped column through an ALIAS';
DROP TABLE IF EXISTS t_retype_default_alias;
CREATE TABLE t_retype_default_alias (id UInt64, b Int32, a Int64 ALIAS toInt64(b)) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_retype_default_alias SELECT number, toInt32(1073741825 + number * 2) FROM numbers(4);
-- metadata-only add, so the part never stores c; its DEFAULT reaches `b` only through the ALIAS `a`
ALTER TABLE t_retype_default_alias ADD COLUMN c Int64 DEFAULT a SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_default_alias ADD PROJECTION p (SELECT id, c ORDER BY id) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_default_alias MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'P c not stored by the part', count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_retype_default_alias' AND active AND column = 'c';
SELECT 'P c stored by the projection', count() FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_retype_default_alias' AND active AND name = 'p' AND column = 'c';
-- positive control BEFORE the retype: the projection is materialised and is chosen under force, so
-- the refusal below cannot pass by the projection never having held a value
SELECT 'P control, projection answers before the retype', c, count() AS n FROM t_retype_default_alias
GROUP BY c ORDER BY c SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SYSTEM STOP MERGES t_retype_default_alias;
ALTER TABLE t_retype_default_alias MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'P authoritative', c, count() AS n FROM t_retype_default_alias GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 0;
SELECT c, count() AS n FROM t_retype_default_alias GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
DROP TABLE t_retype_default_alias;

SELECT '-- M. the changed column is read as part of a bigger column';
DROP TABLE IF EXISTS t_retype_subcolumn;
CREATE TABLE t_retype_subcolumn
(
    id UInt64,
    t Tuple(a Int32, b String),
    PROJECTION p (SELECT toInt64(t.a), count() GROUP BY toInt64(t.a))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, materialize_projections_on_insert = 0;
INSERT INTO t_retype_subcolumn SELECT number, (toInt32(1073741825 + number * 2), 'x') FROM numbers(4);
ALTER TABLE t_retype_subcolumn MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_retype_subcolumn;
ALTER TABLE t_retype_subcolumn MODIFY COLUMN t Tuple(a Float32, b String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'M authoritative', toInt64(t.a) AS k, count() AS c FROM t_retype_subcolumn GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'M with projections', toInt64(t.a) AS k, count() AS c FROM t_retype_subcolumn GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
DROP TABLE t_retype_subcolumn;

SELECT '-- H1. a column the part holds no values of because every value was the type default';
DROP TABLE IF EXISTS t_retype_skipped;
CREATE TABLE t_retype_skipped
(
    id UInt64,
    dt DateTime('UTC'),
    PROJECTION p (SELECT toHour(dt), count() GROUP BY toHour(dt))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0,
         skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
-- every value is the type default, so the column itself is not written; the part records the type
-- its default was frozen from
INSERT INTO t_retype_skipped SELECT number, toDateTime(0, 'UTC') FROM numbers(4);
SELECT 'H1 column not stored', count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_retype_skipped' AND active AND column = 'dt';
ALTER TABLE t_retype_skipped MODIFY COLUMN dt DateTime('Asia/Tokyo') SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'H1 authoritative', toHour(dt) AS h, count() AS c FROM t_retype_skipped GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 0;
SELECT 'H1 with projections', toHour(dt) AS h, count() AS c FROM t_retype_skipped GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 1;
DROP TABLE t_retype_skipped;

SELECT '-- H2. a column added after the part was written, so the part records no type for it';
DROP TABLE IF EXISTS t_retype_added;
CREATE TABLE t_retype_added (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_retype_added SELECT number FROM numbers(4);
-- adding a column with a default to a wide part writes no data, so the part holds no values of it
ALTER TABLE t_retype_added ADD COLUMN b Int32 DEFAULT 1073741825 SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_added ADD PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b)) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_added MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'H2 column not stored', count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_retype_added' AND active AND column = 'b';
SYSTEM STOP MERGES t_retype_added;
ALTER TABLE t_retype_added MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'H2 authoritative', toInt64(b) AS k, count() AS c FROM t_retype_added GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'H2 with projections', toInt64(b) AS k, count() AS c FROM t_retype_added GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
DROP TABLE t_retype_added;

SELECT '-- N. the price of refusing when the part records no type: the answer is right, the';
SELECT '--    projection is skipped until a merge rewrites the part';
DROP TABLE IF EXISTS t_retype_no_change;
CREATE TABLE t_retype_no_change (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_retype_no_change SELECT number FROM numbers(4);
ALTER TABLE t_retype_no_change ADD COLUMN b Int32 DEFAULT 1073741825 SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_no_change ADD PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b)) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_no_change MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_retype_no_change;
SELECT 'N answer', toInt64(b) AS k, count() AS c FROM t_retype_no_change GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
SELECT toInt64(b) AS k, count() AS c FROM t_retype_no_change GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
-- a second part carries the column, and merging the two rewrites the projection from current data
INSERT INTO t_retype_no_change (id) SELECT 100 + number FROM numbers(2);
SYSTEM START MERGES t_retype_no_change;
OPTIMIZE TABLE t_retype_no_change FINAL;
SELECT 'N after merge', toInt64(b) AS k, count() AS c FROM t_retype_no_change GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_retype_no_change;

SELECT '-- J. the same price for a type change that happens to keep every value: the projection is';
SELECT '--    skipped although its stored keys are still right. This is deliberate: whether the';
SELECT '--    stored values survived a conversion cannot be decided without recomputing them, and';
SELECT '--    the previous case shows how rewriting the part brings the projection back';
DROP TABLE IF EXISTS t_retype_widen;
CREATE TABLE t_retype_widen
(
    id UInt64,
    b Int32,
    PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
INSERT INTO t_retype_widen SELECT number, toInt32(1073741825 + number * 2) FROM numbers(4);
SYSTEM STOP MERGES t_retype_widen;
ALTER TABLE t_retype_widen MODIFY COLUMN b Int64 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'J authoritative', toInt64(b) AS k, count() AS c FROM t_retype_widen GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'J with projections', toInt64(b) AS k, count() AS c FROM t_retype_widen GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
SELECT toInt64(b) AS k, count() AS c FROM t_retype_widen GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
DROP TABLE t_retype_widen;

SELECT '-- Q. an intermediate of a DEFAULT chain that the part does not record cannot be shown NOT to';
SELECT '--    be staleness, so it is refused even with nothing retyped: the projection froze a value';
SELECT '--    computed through that intermediate under a declaration no part recorded. This is the';
SELECT '--    price arms N and J price, and rewriting the part brings the projection back the way';
SELECT '--    arm N shows';
DROP TABLE IF EXISTS t_retype_default_chain;
CREATE TABLE t_retype_default_chain (id UInt64, b Int32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_retype_default_chain SELECT number, toInt32(1073741825 + number * 2) FROM numbers(4);
-- two metadata-only adds, so the part records neither; `c` reaches `b` only through `d`
ALTER TABLE t_retype_default_chain ADD COLUMN d Int64 DEFAULT toInt64(b) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_default_chain ADD COLUMN c Int64 DEFAULT d SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_default_chain ADD PROJECTION p (SELECT id, c ORDER BY id) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_default_chain MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'Q neither d nor c stored by the part', count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_retype_default_chain' AND active AND column IN ('c', 'd');
-- `d` is neither recorded by the part nor stored by the projection, and it is not an output of the
-- projection either, so no type on disk says what `c` was frozen under: the projection is refused
-- although nothing has been retyped yet
SELECT c, count() AS n FROM t_retype_default_chain
GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
SYSTEM STOP MERGES t_retype_default_chain;
ALTER TABLE t_retype_default_chain MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'Q authoritative', c, count() AS n FROM t_retype_default_chain GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 0;
-- the chain still reaches the retyped `b`, so the refusal must fire once the type does change
SELECT c, count() AS n FROM t_retype_default_chain GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
DROP TABLE t_retype_default_chain;

SELECT '-- R. a filtered projection keeps answering when only a pass-through column is retyped';
-- control: unlike every other arm here, this one does NOT change behaviour without the fix, because it
-- asserts that a projection which is NOT stale is still used
DROP TABLE IF EXISTS t_retype_filtered;
CREATE TABLE t_retype_filtered
(
    id UInt64,
    dt DateTime('UTC'),
    PROJECTION p (SELECT id, dt WHERE id % 2 = 0 ORDER BY id)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
INSERT INTO t_retype_filtered SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR
FROM numbers(8);
-- the WHERE does not read `dt`, and a time zone change rewrites no data, so the stored rows and the
-- stored order are both still current and the projection's own copy of `dt` converts exactly as the
-- parent's does
ALTER TABLE t_retype_filtered MODIFY COLUMN dt DateTime('Asia/Tokyo') SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'R mutations', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_retype_filtered';
SELECT 'R authoritative', id, toHour(dt) AS h FROM t_retype_filtered WHERE id % 2 = 0 ORDER BY id
SETTINGS optimize_use_projections = 0;
SELECT 'R with the projection', id, toHour(dt) AS h FROM t_retype_filtered WHERE id % 2 = 0 ORDER BY id
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_retype_filtered;

SELECT '-- S. a normal projection whose sort key is a column the part does not record: the stored';
SELECT '--    copy and the order over it were both frozen under the type declared back then';
DROP TABLE IF EXISTS t_retype_missing_key;
CREATE TABLE t_retype_missing_key (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
-- a materialised `_block_number` / `_block_offset` makes a filter on a column the part does not
-- record match no rows at all, although an unfiltered read returns the very values the filter asks
-- for, which is wrong before any projection is considered and is not this test's subject
         enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_retype_missing_key SELECT number FROM numbers(4);
-- a metadata-only add, so the part records no `dt` at all, and the projection freezes what the
-- DEFAULT produced under DateTime('UTC') together with the order over it
ALTER TABLE t_retype_missing_key ADD COLUMN dt DateTime('UTC') DEFAULT '2026-01-01 05:00:00'
SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_missing_key ADD PROJECTION p (SELECT id, dt ORDER BY dt) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_missing_key MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'S dt not stored by the part', count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_retype_missing_key' AND active AND column = 'dt';
SELECT 'S dt recorded by the projection', type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_retype_missing_key' AND active AND name = 'p' AND column = 'dt';
-- positive control BEFORE the retype: the projection answers, so the assertions below cannot pass by
-- the projection never having held a value
SELECT 'S control, projection answers before the retype', count() FROM t_retype_missing_key
WHERE dt = toDateTime('2026-01-01 05:00:00', 'UTC')
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         use_query_condition_cache = 0, optimize_trivial_count_query = 0;
ALTER TABLE t_retype_missing_key MODIFY COLUMN dt DateTime('Asia/Tokyo')
SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'S authoritative', count(), min(toUInt32(dt)) FROM t_retype_missing_key
WHERE dt = toDateTime('2026-01-01 05:00:00', 'Asia/Tokyo')
SETTINGS optimize_use_projections = 0, use_query_condition_cache = 0, optimize_trivial_count_query = 0;
SELECT 'S with projections', count(), min(toUInt32(dt)) FROM t_retype_missing_key
WHERE dt = toDateTime('2026-01-01 05:00:00', 'Asia/Tokyo')
SETTINGS optimize_use_projections = 1, use_query_condition_cache = 0, optimize_trivial_count_query = 0;
SELECT count(), min(toUInt32(dt)) FROM t_retype_missing_key
WHERE dt = toDateTime('2026-01-01 05:00:00', 'Asia/Tokyo')
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         use_query_condition_cache = 0, optimize_trivial_count_query = 0; -- { serverError PROJECTION_NOT_USED }
DROP TABLE t_retype_missing_key;

SELECT '-- T. a filtered projection whose WHERE reads a column the part does not record: the row set';
SELECT '--    it kept was decided under the type declared back then';
DROP TABLE IF EXISTS t_retype_missing_filter;
CREATE TABLE t_retype_missing_filter (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
-- the two block columns are pinned off for the reason given in arm S
         enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_retype_missing_filter SELECT number FROM numbers(4);
-- no DEFAULT expression, so both read paths fall back to the type's own default; that instant is
-- hour 0 in UTC, where the WHERE keeps only `id = 0`, and hour 9 in Asia/Tokyo, where it keeps all
ALTER TABLE t_retype_missing_filter ADD COLUMN dt DateTime('UTC') SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_missing_filter ADD PROJECTION p
    (SELECT id, dt WHERE id = 0 OR toHour(dt) = 9 ORDER BY id) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_missing_filter MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'T projection rows', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_retype_missing_filter' AND active AND name = 'p';
SELECT 'T control, projection answers before the retype', count() FROM t_retype_missing_filter
WHERE id = 0 OR toHour(dt) = 9
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         use_query_condition_cache = 0, optimize_trivial_count_query = 0;
ALTER TABLE t_retype_missing_filter MODIFY COLUMN dt DateTime('Asia/Tokyo')
SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'T authoritative', count() FROM t_retype_missing_filter WHERE id = 0 OR toHour(dt) = 9
SETTINGS optimize_use_projections = 0, use_query_condition_cache = 0, optimize_trivial_count_query = 0;
SELECT 'T with projections', count() FROM t_retype_missing_filter WHERE id = 0 OR toHour(dt) = 9
SETTINGS optimize_use_projections = 1, use_query_condition_cache = 0, optimize_trivial_count_query = 0;
-- this query's own filter does not imply the projection's, so at default settings the projection is
-- not considered and the stored row set is observable only when it is asked for explicitly
SELECT count() FROM t_retype_missing_filter WHERE id = 0 OR toHour(dt) = 9
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         use_query_condition_cache = 0, optimize_trivial_count_query = 0; -- { serverError PROJECTION_NOT_USED }
DROP TABLE t_retype_missing_filter;

SELECT '-- V. an aggregate projection grouped by a column the part does not record: its keys are what';
SELECT '--    that column''s DEFAULT produced from the retyped column back then';
DROP TABLE IF EXISTS t_retype_agg_default;
CREATE TABLE t_retype_agg_default (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
-- the two block columns are pinned off for the reason given in arm S
         enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_retype_agg_default SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR
FROM numbers(4);
-- a metadata-only add, so the part records no `h`; the projection then freezes the hours the DEFAULT
-- produced while `dt` read as DateTime('UTC')
ALTER TABLE t_retype_agg_default ADD COLUMN h UInt8 DEFAULT toHour(dt) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_agg_default ADD PROJECTION p (SELECT h, count() GROUP BY h) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_agg_default MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'V h not stored by the part', count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_retype_agg_default' AND active AND column = 'h';
SELECT 'V h recorded by the projection', type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_retype_agg_default' AND active AND name = 'p' AND column = 'h';
-- positive control BEFORE the retype: the projection answers, so the assertions below cannot pass by
-- the projection never having held a value
SELECT 'V control, projection answers before the retype', h, count() AS n FROM t_retype_agg_default
GROUP BY h ORDER BY h SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
ALTER TABLE t_retype_agg_default MODIFY COLUMN dt DateTime('Asia/Tokyo') SETTINGS mutations_sync = 2, alter_sync = 2;
-- a time zone change rewrites no data, so no mutation exists that could refresh the stored hours
SELECT 'V no mutation for the retype', count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_retype_agg_default' AND command ILIKE '%MODIFY COLUMN%';
SELECT 'V authoritative', h, count() AS n FROM t_retype_agg_default GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 0;
SELECT 'V with projections', h, count() AS n FROM t_retype_agg_default GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 1;
SELECT h, count() AS n FROM t_retype_agg_default GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
DROP TABLE t_retype_agg_default;

SELECT '-- W. a normal projection storing a DEFAULT output whose input neither part records: the';
SELECT '--    stored value was computed while that input read as its old declaration, and no type on';
SELECT '--    disk says which one that was, so this shape is refused for the same reason as arm Q';
DROP TABLE IF EXISTS t_retype_unrecorded_input;
CREATE TABLE t_retype_unrecorded_input (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
-- the two block columns are pinned off for the reason given in arm S
         enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_retype_unrecorded_input SELECT number FROM numbers(4);
-- both adds are metadata-only, so the part records neither column; `b`'s DEFAULT is the epoch, which
-- is hour 0 while `b` reads as UTC and hour 9 once it reads as Tokyo
ALTER TABLE t_retype_unrecorded_input ADD COLUMN b DateTime('UTC') DEFAULT toDateTime(0, 'UTC')
SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_unrecorded_input ADD COLUMN c UInt8 DEFAULT toHour(b) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_retype_unrecorded_input ADD PROJECTION p (SELECT id, c ORDER BY id) SETTINGS alter_sync = 2;
ALTER TABLE t_retype_unrecorded_input MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'W neither b nor c stored by the part', count() = 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_retype_unrecorded_input' AND active AND column IN ('b', 'c');
-- the projection did hold a value of `c`, so the refusal below is not vacuous. A read control before
-- the retype cannot show it here: `b` is recorded by no part at any time, so this projection is
-- refused from the moment it is materialised, exactly as in arm Q
SELECT 'W c stored by the projection', column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_retype_unrecorded_input' AND active AND name = 'p' AND column = 'c';
SELECT 'W projection rows', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_retype_unrecorded_input' AND active AND name = 'p';
ALTER TABLE t_retype_unrecorded_input MODIFY COLUMN b DateTime('Asia/Tokyo') SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'W authoritative', c, count() AS n FROM t_retype_unrecorded_input GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 0;
SELECT 'W with projections', c, count() AS n FROM t_retype_unrecorded_input GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 1;
SELECT c, count() AS n FROM t_retype_unrecorded_input GROUP BY c ORDER BY c
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }
DROP TABLE t_retype_unrecorded_input;
