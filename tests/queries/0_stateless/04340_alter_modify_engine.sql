-- Tags: no-random-merge-tree-settings, no-shared-merge-tree
-- ^ no-shared-merge-tree: MODIFY ENGINE is not supported for Replicated/Shared MergeTree yet.

-- ALTER TABLE ... MODIFY ENGINE changes a MergeTree-family table's engine in place (issue #107551).
-- It rewrites only the merge semantics (MergingParams); ORDER BY / columns are untouched. The change
-- is persisted into the CREATE query and takes effect when the storage is next loaded, so the test
-- DETACHes and ATTACHes the table to observe the new engine's behavior.

SET allow_experimental_alter_modify_engine = 0;

DROP TABLE IF EXISTS t_engine;
CREATE TABLE t_engine (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;

-- Gated behind the experimental setting.
ALTER TABLE t_engine MODIFY ENGINE = ReplacingMergeTree; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_alter_modify_engine = 1;

-- MergeTree -> ReplacingMergeTree. Duplicate ORDER BY keys survive until FINAL collapses them.
INSERT INTO t_engine VALUES (1, 10);
INSERT INTO t_engine VALUES (1, 20);
INSERT INTO t_engine VALUES (2, 5);
SELECT 'before', count() FROM t_engine;

ALTER TABLE t_engine MODIFY ENGINE = ReplacingMergeTree;
DETACH TABLE t_engine;
ATTACH TABLE t_engine;
SELECT 'engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_engine';
SELECT 'replacing final', count() FROM t_engine FINAL;

DROP TABLE t_engine;

-- MergeTree -> SummingMergeTree. FINAL collapses the per-key rows into one summed row. Selecting v
-- directly (not sum(v)) proves the engine changed: plain MergeTree does not support FINAL at all.
CREATE TABLE t_sum (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sum VALUES (1, 10);
INSERT INTO t_sum VALUES (1, 20);
INSERT INTO t_sum VALUES (2, 100);
ALTER TABLE t_sum MODIFY ENGINE = SummingMergeTree;
DETACH TABLE t_sum;
ATTACH TABLE t_sum;
SELECT 'summing', k, v FROM t_sum FINAL ORDER BY k;
DROP TABLE t_sum;

-- Adding the engine-required column in the same statement, before MODIFY ENGINE.
CREATE TABLE t_collapse (a UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_collapse ADD COLUMN sign Int8 DEFAULT 1, MODIFY ENGINE = CollapsingMergeTree(sign);
DETACH TABLE t_collapse;
ATTACH TABLE t_collapse;
SELECT 'collapsing', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_collapse';
DROP TABLE t_collapse;

-- Validation: the target must be a MergeTree-family engine and its required columns must exist.
CREATE TABLE t_bad (a UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_bad MODIFY ENGINE = Log; -- { serverError UNKNOWN_STORAGE }
ALTER TABLE t_bad MODIFY ENGINE = CollapsingMergeTree(missing); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DROP TABLE t_bad;

-- Old-syntax tables keep the key/granularity as positional engine arguments. Rewriting the engine
-- clause would drop them and leave an unloadable CREATE query, so MODIFY ENGINE rejects them.
SET allow_deprecated_syntax_for_merge_tree = 1;
CREATE TABLE t_old (d Date, k UInt32, v UInt32) ENGINE = MergeTree(d, k, 8192);
ALTER TABLE t_old MODIFY ENGINE = ReplacingMergeTree(v); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_old;

-- allow_tuple_element_aggregation (Summing/Aggregating/Coalescing only) is derived from the table's
-- final MergeTree settings, not from the engine clause. MODIFY ENGINE must validate the candidate
-- against it, otherwise a table with a Tuple sorting key would pass the ALTER and fail on next ATTACH.

-- (a) setting already on the table: switching to Summing makes the Tuple sorting key illegal.
CREATE TABLE t_tea (k Tuple(UInt32, UInt32), v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_tea MODIFY ENGINE = SummingMergeTree; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_tea;

-- (b) setting flipped on in the same statement: validated against the in-flight value.
CREATE TABLE t_tea (k Tuple(UInt32, UInt32), v UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_tea MODIFY ENGINE = AggregatingMergeTree, MODIFY SETTING allow_tuple_element_aggregation = 1; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_tea;

-- (c) the flag is ignored for non-aggregating engines, so switching to Replacing stays allowed.
CREATE TABLE t_tea (k Tuple(UInt32, UInt32), v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_tea MODIFY ENGINE = ReplacingMergeTree(v);
DETACH TABLE t_tea;
ATTACH TABLE t_tea;
SELECT 'tuple-key replacing', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_tea';
DROP TABLE t_tea;

-- MODIFY ENGINE chooses a new engine now, so it is create-time metadata (not the legacy grandfathering
-- ATTACH does). The AggregatingMergeTree off-key-dimension guard (issue #751) must apply: a column that
-- is neither in the sorting key nor an aggregate measure keeps an arbitrary value after merges, silently
-- producing wrong results. A plain CREATE AggregatingMergeTree rejects such a schema; MODIFY ENGINE must too.

-- (k) MergeTree -> AggregatingMergeTree with an off-key non-measure dimension column is rejected.
CREATE TABLE t_dim (k UInt64, dim String, m AggregateFunction(sum, UInt64)) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_dim MODIFY ENGINE = AggregatingMergeTree; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_dim;

-- (l) the allow_dimensions_outside_sorting_key escape hatch works on MODIFY ENGINE, as on CREATE.
CREATE TABLE t_dim (k UInt64, dim String, m AggregateFunction(sum, UInt64)) ENGINE = MergeTree ORDER BY k
    SETTINGS allow_dimensions_outside_sorting_key = 1;
ALTER TABLE t_dim MODIFY ENGINE = AggregatingMergeTree;
DETACH TABLE t_dim;
ATTACH TABLE t_dim;
SELECT 'dimension escape hatch', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_dim';
DROP TABLE t_dim;

-- (m) a schema with every column covered (in the sorting key) or a measure is accepted.
CREATE TABLE t_dim (k UInt64, dim String, m AggregateFunction(sum, UInt64)) ENGINE = MergeTree ORDER BY (k, dim);
ALTER TABLE t_dim MODIFY ENGINE = AggregatingMergeTree;
DETACH TABLE t_dim;
ATTACH TABLE t_dim;
SELECT 'dimension in sorting key', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_dim';
DROP TABLE t_dim;

-- (n) a table with no aggregate-state measure is not the issue #751 scenario, so it is accepted.
CREATE TABLE t_dim (k UInt64, dim String) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_dim MODIFY ENGINE = AggregatingMergeTree;
DETACH TABLE t_dim;
ATTACH TABLE t_dim;
SELECT 'no measures', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_dim';
DROP TABLE t_dim;

-- (o) the guard also fires on the reload-path validation: an engine left pending by a reload-only
-- MODIFY ENGINE is re-checked on a later ALTER, so adding the offending column then is rejected too.
CREATE TABLE t_dim (k UInt64, m AggregateFunction(sum, UInt64)) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_dim MODIFY ENGINE = AggregatingMergeTree;
ALTER TABLE t_dim ADD COLUMN dim String; -- { serverError BAD_ARGUMENTS }
DETACH TABLE t_dim;
ATTACH TABLE t_dim;
SELECT 'pending dimension guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_dim';
DROP TABLE t_dim;

-- A reload-only MODIFY ENGINE leaves the new engine pending on the live metadata while merging_params
-- stays the old mode until reload. A subsequent ALTER with no MODIFY ENGINE of its own must re-validate
-- that pending engine before changing any metadata, otherwise it can persist an unloadable CREATE.

-- (d) a later MODIFY SETTING that invalidates the pending engine is rejected; the table stays loadable.
CREATE TABLE t_pending (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY (k, v)
    SETTINGS allow_summing_columns_in_partition_or_order_key = 1;
ALTER TABLE t_pending MODIFY ENGINE = SummingMergeTree(v);
ALTER TABLE t_pending MODIFY SETTING allow_summing_columns_in_partition_or_order_key = 0; -- { serverError BAD_ARGUMENTS }
DETACH TABLE t_pending;
ATTACH TABLE t_pending;
SELECT 'pending setting guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending';
DROP TABLE t_pending;

-- (e) dropping the summing column of the pending engine is rejected before the live metadata changes.
CREATE TABLE t_pending (k UInt32, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_pending MODIFY ENGINE = SummingMergeTree(v);
ALTER TABLE t_pending DROP COLUMN v; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DETACH TABLE t_pending;
ATTACH TABLE t_pending;
SELECT 'pending drop guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending';
DROP TABLE t_pending;

-- The pending-engine validation must build the candidate settings the way reload does (defaults plus
-- the final settings_changes), not as a delta on the current settings. A RESET SETTING removes the
-- setting from settings_changes, so a delta-on-current would keep the old value and miss the conflict,
-- while reload reverts it to the default -- leaving an unloadable CREATE.

-- (k) a later RESET SETTING that reverts the flag to its default invalidates the pending Summing engine.
CREATE TABLE t_pending (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY (k, v)
    SETTINGS allow_summing_columns_in_partition_or_order_key = 1;
ALTER TABLE t_pending MODIFY ENGINE = SummingMergeTree(v);
ALTER TABLE t_pending RESET SETTING allow_summing_columns_in_partition_or_order_key; -- { serverError BAD_ARGUMENTS }
DETACH TABLE t_pending;
ATTACH TABLE t_pending;
SELECT 'pending reset guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending';
DROP TABLE t_pending;

-- (l) MODIFY ENGINE and RESET SETTING in the same statement: validated against the reset (default) value.
CREATE TABLE t_pending (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY (k, v)
    SETTINGS allow_summing_columns_in_partition_or_order_key = 1;
ALTER TABLE t_pending MODIFY ENGINE = SummingMergeTree(v), RESET SETTING allow_summing_columns_in_partition_or_order_key; -- { serverError BAD_ARGUMENTS }
DETACH TABLE t_pending;
ATTACH TABLE t_pending;
SELECT 'reset same stmt guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending';
DROP TABLE t_pending;

-- (f) an unrelated later ALTER on a table with a pending engine still works.
CREATE TABLE t_pending (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_pending MODIFY ENGINE = SummingMergeTree(v);
ALTER TABLE t_pending ADD COLUMN z UInt8;
DETACH TABLE t_pending;
ATTACH TABLE t_pending;
SELECT 'pending unrelated alter', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending';
DROP TABLE t_pending;

-- registerStorageMergeTree rejects a special-mode MergeTree with projections when
-- deduplicate_merge_projection_mode = throw (the default). MODIFY ENGINE must apply the same check,
-- otherwise switching to a special mode while a projection is present (or adding one in the same/a
-- later ALTER) persists a CREATE that fails on the next ATTACH.

-- (g) existing projection + MODIFY ENGINE to a special mode is rejected; the table stays loadable.
CREATE TABLE t_proj (k UInt32, v UInt64, PROJECTION p (SELECT k, sum(v) GROUP BY k)) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_proj MODIFY ENGINE = ReplacingMergeTree; -- { serverError SUPPORT_IS_DISABLED }
DETACH TABLE t_proj;
ATTACH TABLE t_proj;
SELECT 'projection guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_proj';
DROP TABLE t_proj;

-- (h) MODIFY ENGINE to a special mode and ADD PROJECTION in the same statement is rejected.
CREATE TABLE t_proj (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_proj ADD PROJECTION p (SELECT k, sum(v) GROUP BY k), MODIFY ENGINE = ReplacingMergeTree; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE t_proj;

-- (i) adding a projection to a table with a pending special engine is rejected before metadata changes.
CREATE TABLE t_proj (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_proj MODIFY ENGINE = ReplacingMergeTree;
ALTER TABLE t_proj ADD PROJECTION p (SELECT k, sum(v) GROUP BY k); -- { serverError SUPPORT_IS_DISABLED }
DETACH TABLE t_proj;
ATTACH TABLE t_proj;
SELECT 'pending projection guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_proj';
DROP TABLE t_proj;

-- (j) with deduplicate_merge_projection_mode = rebuild the special mode + projection is allowed.
CREATE TABLE t_proj (k UInt32, v UInt64, PROJECTION p (SELECT k, sum(v) GROUP BY k)) ENGINE = MergeTree ORDER BY k
    SETTINGS deduplicate_merge_projection_mode = 'rebuild';
ALTER TABLE t_proj MODIFY ENGINE = ReplacingMergeTree;
DETACH TABLE t_proj;
ATTACH TABLE t_proj;
SELECT 'projection rebuild', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_proj';
DROP TABLE t_proj;

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
CREATE TABLE t_graphite_time (key UInt32, Path String, Time Date, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_time VALUES (1, 'max_a', toDate('2020-01-01'), 1, 1);
INSERT INTO t_graphite_time VALUES (1, 'max_a', toDate('2020-01-01'), 5, 2);
ALTER TABLE t_graphite_time MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_time;
ATTACH TABLE t_graphite_time;
OPTIMIZE TABLE t_graphite_time FINAL;
SELECT 'graphite time Date', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_graphite_time';
SELECT 'graphite time Date rollup', Path, toString(Time), Value, Version FROM t_graphite_time ORDER BY Time;
DROP TABLE t_graphite_time;

CREATE TABLE t_graphite_time (key UInt32, Path String, Time UInt32, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_time VALUES (1, 'max_a', 10, 1, 1);
INSERT INTO t_graphite_time VALUES (1, 'max_a', 80, 5, 2);
ALTER TABLE t_graphite_time MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_time;
ATTACH TABLE t_graphite_time;
OPTIMIZE TABLE t_graphite_time FINAL;
SELECT 'graphite time UInt32', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_graphite_time';
SELECT 'graphite time UInt32 rollup', Path, toString(Time), Value, Version FROM t_graphite_time ORDER BY Time;
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

-- A fixed-width `Array` stays allowed: `ColumnArray::getDataAt` reads it, so the rollup works and
-- rejecting it would make MODIFY ENGINE stricter than CREATE TABLE.
CREATE TABLE t_graphite_path (key UInt32, Path Array(UInt32), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, [1, 2], '2020-01-01 00:00:00', 5, 1);
INSERT INTO t_graphite_path VALUES (1, [1, 2], '2020-01-01 00:00:10', 6, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_path;
ATTACH TABLE t_graphite_path;
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite fixed array path rollup', Path, toString(Time), Value, Version FROM t_graphite_path ORDER BY Time;
DROP TABLE t_graphite_path;

-- A `FixedString`, `LowCardinality(String)` or `Enum` path stays allowed: all three implement
-- `getDataAt`, so the rollup reads them and the merge collapses the two versions.
CREATE TABLE t_graphite_path (key UInt32, Path FixedString(5), Time DateTime, Value Float64, Version UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_graphite_path VALUES (1, 'max_a', '2020-01-01 00:00:00', 5, 1);
INSERT INTO t_graphite_path VALUES (1, 'max_a', '2020-01-01 00:00:10', 6, 2);
ALTER TABLE t_graphite_path MODIFY ENGINE = GraphiteMergeTree('graphite_rollup');
DETACH TABLE t_graphite_path;
ATTACH TABLE t_graphite_path;
OPTIMIZE TABLE t_graphite_path FINAL;
SELECT 'graphite fixedstring path rollup', Path, toString(Time), Value, Version FROM t_graphite_path ORDER BY Time;
DROP TABLE t_graphite_path;

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

-- (s) the engine clause survives a round trip through the `clickhouse_json` AST dialect.
SET enable_json_ast_dialect = 1;
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY ENGINE = ReplacingMergeTree'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY ENGINE = ReplacingMergeTree(v)'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY ENGINE = SummingMergeTree((x, y))'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY ENGINE = GraphiteMergeTree(\'graphite_rollup\')'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD COLUMN sign Int8, MODIFY ENGINE = CollapsingMergeTree(sign)'));

-- (t) a MODIFY_ENGINE command with no engine child fails closed, as the sibling commands do, rather
-- than reaching `AlterCommand::parse` and crashing on the absent engine node.
SELECT formatQueryFromJSON('{"type":"AlterQuery","table":"t","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_ENGINE"}]}}'); -- { serverError BAD_ARGUMENTS }

-- (u) a non-ASTFunction engine child is rejected at the JSON boundary, not as an internal cast error.
SELECT formatQueryFromJSON('{"type":"AlterQuery","table":"t","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_ENGINE","engine":{"type":"Identifier","name":"ReplacingMergeTree"}}]}}'); -- { serverError BAD_ARGUMENTS }
