-- Tags: no-random-merge-tree-settings, no-shared-merge-tree
-- ^ no-shared-merge-tree: MODIFY ENGINE is not supported for Replicated/Shared MergeTree yet.

-- Graphite (and the remaining special-mode target engines) half of the ALTER TABLE ... MODIFY ENGINE
-- coverage. The non-Graphite arms live in 04340_alter_modify_engine.

SET allow_experimental_alter_modify_engine = 1;
-- Every arm below inserts a handful of rows and then asserts what the next merge produces, so each
-- INSERT must be a part before the following statement runs.
SET async_insert = 0;


-- Graphite schema is validated up front on MODIFY ENGINE, matching what the rollup algorithm needs at
-- merge time (configured path/time/value/version columns exist and the value column is Float64). The
-- `graphite_rollup` config element uses the default Path/Time/Value column names and version_column_name = Version.

-- (p) a non-Float64 value column is rejected (would otherwise only fail on the first merge).
CREATE TABLE t_graphite (Path String, Time DateTime, Value UInt64, Version UInt32, key UInt32) ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_graphite MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_graphite;

-- (q) a missing required column (no Version) is rejected.
CREATE TABLE t_graphite (Path String, Time DateTime, Value Float64, key UInt32) ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_graphite MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DROP TABLE t_graphite;

-- (r) a valid Graphite schema is accepted and the engine switches on reload.
CREATE TABLE t_graphite (Path String, Time DateTime, Value Float64, Version UInt32, key UInt32) ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_graphite MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite;
ATTACH TABLE t_graphite;
SELECT 'graphite valid', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_graphite';
DROP TABLE t_graphite;

-- (v) merge semantics, not just the persisted engine name, change for the remaining supported targets.
-- Plain MergeTree rejects FINAL outright, so a change that persisted the name without switching the
-- merge mode would fail these instead of passing them.
-- The version column is in the sorting key, as the check below requires.
CREATE TABLE t_vcollapse (k UInt32, sign Int8, ver UInt32) ENGINE = MergeTree ORDER BY (k, ver);
INSERT INTO t_vcollapse VALUES (1, 1, 1);
INSERT INTO t_vcollapse VALUES (1, -1, 1);
INSERT INTO t_vcollapse VALUES (2, 1, 1);
ALTER TABLE t_vcollapse MODIFY ENGINE = VersionedCollapsingMergeTree(sign, ver);
DETACH TABLE t_vcollapse;
ATTACH TABLE t_vcollapse;
SELECT 'vcollapsing final', k FROM t_vcollapse FINAL ORDER BY k;
DROP TABLE t_vcollapse;

-- (v2) VersionedCollapsingMergeTree is rejected when the version column is outside the sorting key.
-- Its reload would append the column to the key, but the parts inserted above were written under the
-- narrower key, so merging them would read unsorted input. The rows exist before the switch, so this
-- case fails if the check goes away: the ALTER succeeds and the OPTIMIZE below aborts the merge.
CREATE TABLE t_vcollapse (k UInt32, sign Int8, ver UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_vcollapse VALUES (1, 1, 5);
INSERT INTO t_vcollapse VALUES (1, -1, 2);
ALTER TABLE t_vcollapse MODIFY ENGINE = VersionedCollapsingMergeTree(sign, ver); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_vcollapse FINAL;
SELECT 'vcollapsing key guard', engine, sorting_key FROM system.tables
    WHERE database = currentDatabase() AND name = 't_vcollapse';
DROP TABLE t_vcollapse;

CREATE TABLE t_coalesce (k UInt32, a Nullable(UInt32), b Nullable(UInt32)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_coalesce VALUES (1, 10, NULL);
INSERT INTO t_coalesce VALUES (1, NULL, 20);
ALTER TABLE t_coalesce MODIFY ENGINE = CoalescingMergeTree;
DETACH TABLE t_coalesce;
ATTACH TABLE t_coalesce;
SELECT 'coalescing final', k, a, b FROM t_coalesce FINAL ORDER BY k;
DROP TABLE t_coalesce;

CREATE TABLE t_collapse_final (k UInt32, sign Int8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_collapse_final VALUES (1, 1);
INSERT INTO t_collapse_final VALUES (1, -1);
INSERT INTO t_collapse_final VALUES (2, 1);
ALTER TABLE t_collapse_final MODIFY ENGINE = CollapsingMergeTree(sign);
DETACH TABLE t_collapse_final;
ATTACH TABLE t_collapse_final;
SELECT 'collapsing final', k FROM t_collapse_final FINAL ORDER BY k;
DROP TABLE t_collapse_final;

CREATE TABLE t_agg_final (k UInt32, s AggregateFunction(sum, UInt32)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_agg_final SELECT 1, sumState(toUInt32(5));
INSERT INTO t_agg_final SELECT 1, sumState(toUInt32(7));
ALTER TABLE t_agg_final MODIFY ENGINE = AggregatingMergeTree;
DETACH TABLE t_agg_final;
ATTACH TABLE t_agg_final;
SELECT 'aggregating final', k, sumMerge(s) FROM t_agg_final FINAL GROUP BY k ORDER BY k;
DROP TABLE t_agg_final;

-- (w) the optional engine arguments are carried through, not just accepted: `ReplacingMergeTree(ver, del)`
-- selects the highest-version row and drops a row marked deleted, and `SummingMergeTree(x)` sums only the
-- listed column. Dropping or reordering an optional argument changes these results.
CREATE TABLE t_rep_del (k UInt32, v UInt32, ver UInt32, del UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_rep_del VALUES (1, 100, 1, 0);
INSERT INTO t_rep_del VALUES (1, 200, 2, 0);
INSERT INTO t_rep_del VALUES (2, 300, 1, 0);
INSERT INTO t_rep_del VALUES (2, 400, 2, 1);
ALTER TABLE t_rep_del MODIFY ENGINE = ReplacingMergeTree(ver, del);
DETACH TABLE t_rep_del;
ATTACH TABLE t_rep_del;
SELECT 'replacing is_deleted', k, v, ver, del FROM t_rep_del FINAL ORDER BY k;
DROP TABLE t_rep_del;

CREATE TABLE t_sum_explicit (k UInt32, x UInt64, y UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sum_explicit VALUES (1, 10, 7);
INSERT INTO t_sum_explicit VALUES (1, 20, 9);
ALTER TABLE t_sum_explicit MODIFY ENGINE = SummingMergeTree(x);
DETACH TABLE t_sum_explicit;
ATTACH TABLE t_sum_explicit;
SELECT 'summing explicit', k, x, y FROM t_sum_explicit FINAL ORDER BY k;
DROP TABLE t_sum_explicit;

-- (x) an explicit `CoalescingMergeTree` column list is carried through: only the listed column is
-- coalesced, so the unlisted one keeps its first value (without the argument it would become 9).
CREATE TABLE t_coa_explicit (k UInt32, a Nullable(UInt32), b Nullable(UInt32)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_coa_explicit VALUES (1, 10, 7);
INSERT INTO t_coa_explicit VALUES (1, NULL, 9);
ALTER TABLE t_coa_explicit MODIFY ENGINE = CoalescingMergeTree(a);
DETACH TABLE t_coa_explicit;
ATTACH TABLE t_coa_explicit;
SELECT 'coalescing explicit', k, a, b FROM t_coa_explicit FINAL ORDER BY k;
DROP TABLE t_coa_explicit;

-- (y) the Graphite rollup itself runs after the switch, not just the engine name: two points of one
-- path inside a single 600 second retention window (the `graphite_rollup` config's `age 0` precision)
-- roll into one row whose Time is truncated to the window and whose value is the highest Version.
-- Plain MergeTree keeps both rows unchanged.
CREATE TABLE t_graphite_rollup (key UInt32, Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
    ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_rollup VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:10', 'UTC'), 1, 1);
INSERT INTO t_graphite_rollup VALUES (1, 'max_a', toDateTime('2020-01-01 00:01:20', 'UTC'), 5, 2);
ALTER TABLE t_graphite_rollup MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_rollup;
ATTACH TABLE t_graphite_rollup;
OPTIMIZE TABLE t_graphite_rollup FINAL;
SELECT 'graphite rollup', Path, toString(Time), Value, Version FROM t_graphite_rollup ORDER BY Time;
DROP TABLE t_graphite_rollup;

-- A constant expression names the configuration element, as it does in CREATE TABLE, which evaluates
-- engine arguments before reading them. The stored CREATE query must hold the evaluated literal rather
-- than the expression, so that the next load reads this value instead of resolving it again.
CREATE TABLE t_graphite_expr (key UInt32, Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
    ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_graphite_expr MODIFY ENGINE = GraphiteMergeTree(concat('graphite', '_rollup'));
SELECT 'graphite config name evaluated', position(create_table_query, 'concat') = 0,
    position(create_table_query, 'GraphiteMergeTree(\'graphite_rollup\')') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 't_graphite_expr';
DETACH TABLE t_graphite_expr;
ATTACH TABLE t_graphite_expr;
SELECT 'graphite config name expression', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_graphite_expr';
DROP TABLE t_graphite_expr;

-- An expression that is not constant is still rejected.
CREATE TABLE t_graphite_expr (key UInt32, Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
    ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_graphite_expr MODIFY ENGINE = GraphiteMergeTree(Path); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_graphite_expr;

-- (z) the Graphite time column must be a type the rollup can read: it uses `IColumn::getUInt`, which
-- only the integer-backed columns implement, so `String`, `Float64`, `DateTime64` and `Decimal` are
-- rejected up front instead of aborting the first merge with NOT_IMPLEMENTED or BAD_GET.
CREATE TABLE t_graphite_time (key UInt32, Path String, Time String, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_graphite_time MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_graphite_time;

CREATE TABLE t_graphite_time (key UInt32, Path String, Time DateTime64(3), Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_graphite_time MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_graphite_time;

-- The accepted types must not be rejected by that check: Date and an integer. Both roll up two
-- versions of one path, so a type accepted here but unreadable by `getUInt` at merge time reddens
-- on the OPTIMIZE rather than passing on the engine name alone.
-- The rounded `Time` is not read back: above a retention precision of 900 the rollup rounds
-- relative to the local day, so the rounded value follows the server time zone. The collapse to one
-- row and the surviving version do not.
CREATE TABLE t_graphite_time (key UInt32, Path String, Time Date, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_time VALUES (1, 'max_a', toDate('2020-01-01'), 1, 1);
INSERT INTO t_graphite_time VALUES (1, 'max_a', toDate('2020-01-01'), 5, 2);
ALTER TABLE t_graphite_time MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_time;
ATTACH TABLE t_graphite_time;
OPTIMIZE TABLE t_graphite_time FINAL;
SELECT 'graphite time Date', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_graphite_time';
SELECT 'graphite time Date rollup', Path, count(), Value, Version FROM t_graphite_time GROUP BY Path, Value, Version;
DROP TABLE t_graphite_time;

CREATE TABLE t_graphite_time (key UInt32, Path String, Time UInt32, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_time VALUES (1, 'max_a', 10, 1, 1);
INSERT INTO t_graphite_time VALUES (1, 'max_a', 80, 5, 2);
ALTER TABLE t_graphite_time MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_time;
ATTACH TABLE t_graphite_time;
OPTIMIZE TABLE t_graphite_time FINAL;
SELECT 'graphite time UInt32', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_graphite_time';
SELECT 'graphite time UInt32 rollup', Path, count(), Value, Version FROM t_graphite_time GROUP BY Path, Value, Version;
DROP TABLE t_graphite_time;

-- (z2) a nullable path or time column is rejected too. The rollup reads them with `getDataAt` and
-- `getUInt`, which throw on a NULL, so a single NULL row would leave the table unable to merge and
-- unable to answer FINAL reads. The rows exist before the switch, so these cases redden if the guard
-- goes away: without it the ALTER succeeds and the OPTIMIZE below fails instead.
CREATE TABLE t_graphite_null (key UInt32, Path String, Time Nullable(DateTime), Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_null VALUES (1, 'max_a', NULL, 5, 2);
ALTER TABLE t_graphite_null MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_null FINAL;
SELECT 'graphite nullable time rejected', engine, count() FROM t_graphite_null, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_null' GROUP BY engine;
DROP TABLE t_graphite_null;

CREATE TABLE t_graphite_null (key UInt32, Path Nullable(String), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_null VALUES (1, NULL, '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_null MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_null FINAL;
SELECT 'graphite nullable path rejected', engine, count() FROM t_graphite_null, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_null' GROUP BY engine;
DROP TABLE t_graphite_null;

-- (z3) a composite path column is rejected: the rollup reads the path with `getDataAt`, which these
-- columns do not implement. Unlike CREATE TABLE, which cannot reach this state because the same method
-- rejects the INSERT, MODIFY ENGINE arrives at an already-populated table, so accepting it would leave
-- the table unable to merge and unable to answer a FINAL read.
CREATE TABLE t_graphite_path (key UInt32, Path Array(String), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, ['max_a'], '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite array path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;

CREATE TABLE t_graphite_path (key UInt32, Path Tuple(UInt32, UInt32), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, (1, 2), '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite tuple path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;

CREATE TABLE t_graphite_path (key UInt32, Path Map(String, String), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, map('a', 'b'), '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite map path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;

-- `QBit` is a separate type index, but its column forwards `getDataAt` to a `Tuple`, so it fails the
-- same way and must be listed separately from the composite types above.
CREATE TABLE t_graphite_path (key UInt32, Path QBit(BFloat16, 8), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, [1,2,3,4,5,6,7,8], '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite qbit path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;

-- `Variant`, `Dynamic` and `JSON` are separate type indexes that no other term of the guard covers,
-- so each needs its own case.
SET enable_variant_type = 1;
CREATE TABLE t_graphite_path (key UInt32, Path Variant(String, UInt64), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite variant path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;
SET enable_variant_type = 0;

SET enable_dynamic_type = 1;
CREATE TABLE t_graphite_path (key UInt32, Path Dynamic, Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite dynamic path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;
SET enable_dynamic_type = 0;

-- The value must be valid JSON: a bare string fails at INSERT, so the case would never reach the guard.
SET enable_json_type = 1;
CREATE TABLE t_graphite_path (key UInt32, Path JSON, Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, '{"a":"max_a"}', '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite json path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;
SET enable_json_type = 0;

-- The nullability guard uses `isNullableOrLowCardinalityNullable`, so it must also reject a NULL
-- hidden under a `LowCardinality` wrapper; a top-level-only check would pass every case above.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_graphite_path (key UInt32, Path LowCardinality(Nullable(String)), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, NULL, '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite lc nullable path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;
SET allow_suspicious_low_cardinality_types = 0;

-- A nested `LowCardinality` keeps the array unreadable, so asking the original type (not the
-- low-cardinality-stripped one) is load-bearing: `CREATE TABLE` cannot even insert into this shape.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_graphite_path (key UInt32, Path Array(LowCardinality(UInt32)), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, [1, 2], '2020-01-01 00:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite lc array path rejected', engine, count() FROM t_graphite_path, system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path' GROUP BY engine;
DROP TABLE t_graphite_path;
SET allow_suspicious_low_cardinality_types = 0;

-- A fixed-width `Array` stays allowed: `ColumnArray::getDataAt` reads it, so the rollup works and
-- rejecting it would make MODIFY ENGINE stricter than CREATE TABLE.
CREATE TABLE t_graphite_path (key UInt32, Path Array(UInt32), Time DateTime('UTC'), Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, [1, 2], toDateTime('2020-01-01 00:00:00', 'UTC'), 5, 1);
INSERT INTO t_graphite_path VALUES (1, [1, 2], toDateTime('2020-01-01 00:00:10', 'UTC'), 6, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_path;
ATTACH TABLE t_graphite_path;
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite fixed array path rollup', Path, toString(Time), Value, Version FROM t_graphite_path ORDER BY Time;
DROP TABLE t_graphite_path;

-- A `FixedString`, `LowCardinality(String)` or `Enum` path stays allowed: all three implement
-- `getDataAt`, so the rollup reads them and the merge collapses the two versions.
CREATE TABLE t_graphite_path (key UInt32, Path FixedString(5), Time DateTime('UTC'), Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:00', 'UTC'), 5, 1);
INSERT INTO t_graphite_path VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:10', 'UTC'), 6, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_path;
ATTACH TABLE t_graphite_path;
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite fixedstring path rollup', Path, toString(Time), Value, Version FROM t_graphite_path ORDER BY Time;
DROP TABLE t_graphite_path;

CREATE TABLE t_graphite_path (key UInt32, Path LowCardinality(String), Time DateTime('UTC'), Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:00', 'UTC'), 5, 1);
INSERT INTO t_graphite_path VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:10', 'UTC'), 6, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_path;
ATTACH TABLE t_graphite_path;
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite lowcardinality path rollup', Path, toString(Time), Value, Version FROM t_graphite_path ORDER BY Time;
DROP TABLE t_graphite_path;

-- The time classification strips `LowCardinality`, so a wrapped time must still roll up.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_graphite_path (key UInt32, Path String, Time LowCardinality(DateTime('UTC')), Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:00', 'UTC'), 5, 1);
INSERT INTO t_graphite_path VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:10', 'UTC'), 6, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_path;
ATTACH TABLE t_graphite_path;
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite lowcardinality time rollup', Path, count(), Value, Version FROM t_graphite_path GROUP BY Path, Value, Version;
DROP TABLE t_graphite_path;
SET allow_suspicious_low_cardinality_types = 0;

-- `Time` is ColumnVector<Int32>-backed so the rollup reads it, unlike Decimal-backed `Time64`.
CREATE TABLE t_graphite_path (key UInt32, Path String, Time Time, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', '01:00:00', 5, 1);
INSERT INTO t_graphite_path VALUES (1, 'max_a', '01:00:10', 6, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_path;
ATTACH TABLE t_graphite_path;
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite time Time rollup', Path, count(), Value, Version FROM t_graphite_path GROUP BY Path, Value, Version;
DROP TABLE t_graphite_path;

CREATE TABLE t_graphite_path (key UInt32, Path String, Time Time64(3), Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', '01:00:10', 5, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup'); -- { serverError BAD_ARGUMENTS }
SELECT 'graphite time Time64 rejected', engine FROM system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_path';
DROP TABLE t_graphite_path;

-- The time column has a separate `isEnum` branch, so an `Enum` time needs its own case. The rollup
-- writes back the rounded number without checking that the enum can name it, so rendering `Time`
-- here would throw UNKNOWN_ELEMENT_OF_ENUM in most server time zones.
CREATE TABLE t_graphite_path (key UInt32, Path String, Time Enum16('rounded' = 0, 'a' = 600), Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', 'a', 5, 1);
INSERT INTO t_graphite_path VALUES (1, 'max_a', 'a', 6, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_path;
ATTACH TABLE t_graphite_path;
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite enum time rollup', Path, count(), Value, Version FROM t_graphite_path GROUP BY Path, Value, Version;
DROP TABLE t_graphite_path;

-- The candidate sorting key must be rebuilt for the TARGET engine. VersionedCollapsingMergeTree is the
-- only engine that appends its version column to the key, so validating against the live key would
-- credit that column to a target which does not append it, and `ver` would become an off-key dimension
-- after the reload. CREATE TABLE rejects the same shape.
CREATE TABLE t_source_versioned (k UInt32, sign Int8, ver UInt32, m AggregateFunction(sum, UInt64))
    ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY (k, sign);
ALTER TABLE t_source_versioned MODIFY ENGINE = AggregatingMergeTree; -- { serverError BAD_ARGUMENTS }
SELECT 'source versioned dimension rejected', engine FROM system.tables
    WHERE database = currentDatabase() AND name = 't_source_versioned';
-- The rebuild must apply at BOTH validation sites: a target that names the source's implicit key
-- column as a measure is legal (CREATE TABLE accepts it) and must not be rejected as an overlap.
CREATE TABLE t_source_versioned_sum (k UInt32, sign Int8, ver UInt32)
    ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY (k, sign);
ALTER TABLE t_source_versioned_sum MODIFY ENGINE = SummingMergeTree(ver);
DETACH TABLE t_source_versioned_sum;
ATTACH TABLE t_source_versioned_sum;
SELECT 'source versioned to summing', engine, sorting_key FROM system.tables
    WHERE database = currentDatabase() AND name = 't_source_versioned_sum';
DROP TABLE t_source_versioned_sum;

-- The same source engine still converts to a target that needs the version column in the key.
ALTER TABLE t_source_versioned MODIFY ENGINE = ReplacingMergeTree(ver);
DETACH TABLE t_source_versioned;
ATTACH TABLE t_source_versioned;
SELECT 'source versioned to replacing', engine, sorting_key FROM system.tables
    WHERE database = currentDatabase() AND name = 't_source_versioned';
DROP TABLE t_source_versioned;

-- A temporary table is never reloaded, so the new semantics could never take effect.
CREATE TEMPORARY TABLE t_graphite_temp (key UInt32, v UInt32) ENGINE = MergeTree ORDER BY key;
ALTER TABLE t_graphite_temp MODIFY ENGINE = ReplacingMergeTree(v); -- { serverError SUPPORT_IS_DISABLED }
SELECT 'temporary rejected', extract(create_table_query, 'ENGINE = [A-Za-z]+') FROM system.tables
    WHERE name = 't_graphite_temp';
DROP TEMPORARY TABLE t_graphite_temp;

-- A nullable version column stays allowed: the rollup compares it with `compareAt` and copies it with
-- `insertFrom`, both of which handle NULL. The two rows must share the same path and the same unrounded
-- time, because the version comparison is only reached for rows the algorithm considers the same key; two
-- rows merely landing in one retention window skip it. The algorithm compares with a null direction
-- hint of 1, so a NULL version sorts above a set one and its row wins, which is why the asserted Value
-- and Version change if that comparison is wrong rather than only the row count.
CREATE TABLE t_graphite_null (key UInt32, Path String, Time DateTime('UTC'), Value Float64, Version Nullable(UInt32))
    ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_null VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:10', 'UTC'), 1, NULL);
INSERT INTO t_graphite_null VALUES (1, 'max_a', toDateTime('2020-01-01 00:00:10', 'UTC'), 5, 2);
ALTER TABLE t_graphite_null MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_null;
ATTACH TABLE t_graphite_null;
OPTIMIZE TABLE t_graphite_null FINAL;
SELECT 'graphite nullable version accepted', Path, toString(Time), Value, Version FROM t_graphite_null ORDER BY Time;
SELECT 'graphite nullable version engine', engine FROM system.tables
    WHERE database = currentDatabase() AND name = 't_graphite_null';
DROP TABLE t_graphite_null;
