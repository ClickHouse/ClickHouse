-- A projection is written with the implicit min-max indices of its own metadata. It must follow the
-- table's `add_minmax_index_for_*` policy unless its own `WITH SETTINGS` sets the policy explicitly,
-- otherwise a table that opted out of the implicit indices gets them back through its projections.

SET optimize_use_projections = 1, force_optimize_projection = 1, enable_analyzer = 1;

DROP TABLE IF EXISTS t_proj_minmax_off;
DROP TABLE IF EXISTS t_proj_minmax_on;

CREATE TABLE t_proj_minmax_off (a UInt64, c UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0;

INSERT INTO t_proj_minmax_off SELECT number, number * 2, toString(number) FROM numbers(1000);

-- Added with ALTER, after the table exists: inherits the table's opt-out.
ALTER TABLE t_proj_minmax_off ADD PROJECTION p_inherit (SELECT a, c ORDER BY s);
ALTER TABLE t_proj_minmax_off MATERIALIZE PROJECTION p_inherit SETTINGS mutations_sync = 2;

-- The projection's own `WITH SETTINGS` wins over the table's policy.
ALTER TABLE t_proj_minmax_off ADD PROJECTION p_explicit_on (SELECT a, c ORDER BY s) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1);
ALTER TABLE t_proj_minmax_off MATERIALIZE PROJECTION p_explicit_on SETTINGS mutations_sync = 2;

SELECT 'table off, projection inherits';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_proj_minmax_off WHERE s = '777' AND c = 1554 SETTINGS preferred_optimize_projection_name = 'p_inherit')
WHERE explain LIKE '%ReadFromMergeTree (%' OR explain LIKE '%Name: auto_minmax_index%';

SELECT 'table off, projection explicitly on';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_proj_minmax_off WHERE s = '777' AND c = 1554 SETTINGS preferred_optimize_projection_name = 'p_explicit_on')
WHERE explain LIKE '%ReadFromMergeTree (%' OR explain LIKE '%Name: auto_minmax_index%';

CREATE TABLE t_proj_minmax_on (a UInt64, c UInt64, s String,
    PROJECTION p_inherit (SELECT a, c ORDER BY s),
    PROJECTION p_explicit_off (SELECT a, c ORDER BY s) WITH SETTINGS (add_minmax_index_for_numeric_columns = 0))
ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 1, add_minmax_index_for_string_columns = 0;

INSERT INTO t_proj_minmax_on SELECT number, number * 2, toString(number) FROM numbers(1000);

SELECT 'table on, projection inherits';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_proj_minmax_on WHERE s = '777' AND c = 1554 SETTINGS preferred_optimize_projection_name = 'p_inherit')
WHERE explain LIKE '%ReadFromMergeTree (%' OR explain LIKE '%Name: auto_minmax_index%';

SELECT 'table on, projection explicitly off';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_proj_minmax_on WHERE s = '777' AND c = 1554 SETTINGS preferred_optimize_projection_name = 'p_explicit_off')
WHERE explain LIKE '%ReadFromMergeTree (%' OR explain LIKE '%Name: auto_minmax_index%';

DROP TABLE t_proj_minmax_off;
DROP TABLE t_proj_minmax_on;
