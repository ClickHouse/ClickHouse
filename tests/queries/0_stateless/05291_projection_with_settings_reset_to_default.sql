-- `name = DEFAULT` in a projection's `WITH SETTINGS` sets the projection's own default explicitly:
-- the projection must not inherit the table's `add_minmax_index_for_*` policy for that setting.
-- It is kept apart from the other settings in the AST, so the allow-list of projection settings
-- must be checked for it too.

SET optimize_use_projections = 1, force_optimize_projection = 1, enable_analyzer = 1;

DROP TABLE IF EXISTS t_proj_reset;

CREATE TABLE t_proj_reset (a UInt64, c UInt64, s String,
    PROJECTION p_inherit (SELECT a, c ORDER BY s),
    PROJECTION p_reset (SELECT a, c ORDER BY s) WITH SETTINGS (add_minmax_index_for_numeric_columns = DEFAULT))
ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0;

INSERT INTO t_proj_reset SELECT number, number * 2, toString(number) FROM numbers(1000);

SELECT 'projection inherits the opt-out';
SELECT extract(explain, 'ReadFromMergeTree \\(.*\\)|Name: auto_minmax_index.*') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_proj_reset WHERE s = '777' AND c = 1554 SETTINGS preferred_optimize_projection_name = 'p_inherit')
WHERE explain LIKE '%ReadFromMergeTree (%' OR explain LIKE '%Name: auto_minmax_index%';

SELECT 'projection reset to its own default';
SELECT extract(explain, 'ReadFromMergeTree \\(.*\\)|Name: auto_minmax_index.*') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_proj_reset WHERE s = '777' AND c = 1554 SETTINGS preferred_optimize_projection_name = 'p_reset')
WHERE explain LIKE '%ReadFromMergeTree (%' OR explain LIKE '%Name: auto_minmax_index%';

-- The same after the projection metadata is rebuilt from the stored definition.
DETACH TABLE t_proj_reset;
ATTACH TABLE t_proj_reset;

SELECT 'after reattach';
SELECT extract(explain, 'ReadFromMergeTree \\(.*\\)|Name: auto_minmax_index.*') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_proj_reset WHERE s = '777' AND c = 1554 SETTINGS preferred_optimize_projection_name = 'p_reset')
WHERE explain LIKE '%ReadFromMergeTree (%' OR explain LIKE '%Name: auto_minmax_index%';

-- A setting that is not allowed for projections is rejected with `= DEFAULT` as well.
ALTER TABLE t_proj_reset ADD PROJECTION p_bad (SELECT a ORDER BY c) WITH SETTINGS (merge_with_ttl_timeout = DEFAULT); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_proj_reset ADD PROJECTION p_bad (SELECT a ORDER BY c) WITH SETTINGS (index_granularity = DEFAULT);
SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 't_proj_reset' ORDER BY name;

DROP TABLE t_proj_reset;
