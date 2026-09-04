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
-- Every arm below inserts a handful of rows and then asserts what the next merge produces, so each
-- INSERT must be a part before the following statement runs.
SET async_insert = 0;

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

-- The rows must exist BEFORE the ALTER, so this also proves parts written under the old engine can
-- materialize the newly added column and take part in a merge under the new semantics. Asserting only
-- the engine name on an empty table would pass even if they could not.
CREATE TABLE t_required_existing (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_required_existing VALUES (1, 10);
INSERT INTO t_required_existing VALUES (1, 20);
ALTER TABLE t_required_existing ADD COLUMN ver UInt32 DEFAULT v, MODIFY ENGINE = ReplacingMergeTree(ver);
DETACH TABLE t_required_existing;
ATTACH TABLE t_required_existing;
OPTIMIZE TABLE t_required_existing FINAL;
SELECT 'required column existing parts', k, v, ver FROM t_required_existing;
DROP TABLE t_required_existing;

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

-- The candidate-metadata replay must use the table's own `share_nested_offsets`, as `alter()` does.
-- Under the default `true`, `IF NOT EXISTS n` counts the existing `n.a` as `n` and is skipped, so the
-- engine is rejected for a version column this same ALTER does add.
SET flatten_nested = 1;
CREATE TABLE t_nested_offsets (key UInt32, n Nested(a UInt32)) ENGINE = MergeTree ORDER BY key
    SETTINGS share_nested_offsets = 0;
ALTER TABLE t_nested_offsets ADD COLUMN IF NOT EXISTS n UInt32, MODIFY ENGINE = ReplacingMergeTree(n);
DETACH TABLE t_nested_offsets;
ATTACH TABLE t_nested_offsets;
SELECT 'nested offsets engine', engine FROM system.tables
    WHERE database = currentDatabase() AND name = 't_nested_offsets';
SELECT 'nested offsets columns', groupArray(name) FROM system.columns
    WHERE database = currentDatabase() AND table = 't_nested_offsets';
DROP TABLE t_nested_offsets;

-- The replay normalizes statistics first, as the real ALTER paths do. Otherwise the implicit `basic`
-- statistic `auto_statistics_types` generates for every column collides with the explicit one added by
-- this same ALTER, and a valid command is rejected with ILLEGAL_STATISTICS.
CREATE TABLE t_engine_stats (a UInt32, k UInt32) ENGINE = MergeTree ORDER BY a
    SETTINGS auto_statistics_types = 'basic';
ALTER TABLE t_engine_stats MODIFY ENGINE = ReplacingMergeTree, ADD STATISTICS k TYPE basic;
DETACH TABLE t_engine_stats;
ATTACH TABLE t_engine_stats;
SELECT 'implicit statistics engine', engine FROM system.tables
    WHERE database = currentDatabase() AND name = 't_engine_stats';
SELECT 'implicit statistics column', statistics FROM system.columns
    WHERE database = currentDatabase() AND table = 't_engine_stats' AND name = 'k';
DROP TABLE t_engine_stats;

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
