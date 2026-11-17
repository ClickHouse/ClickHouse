-- Test: Projections with WHERE clause (Issue #74234)
-- Verifies filtered projection creation, materialization, and correct query results.

SET optimize_use_projections = 1;

DROP TABLE IF EXISTS t_proj_where;

CREATE TABLE t_proj_where
(
    time DateTime,
    event_type String,
    message String,
    size UInt64
)
ENGINE = MergeTree
ORDER BY time;

-- Create a projection that only materializes pageview events.
ALTER TABLE t_proj_where ADD PROJECTION proj_pageview
(
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

-- Insert mixed data
INSERT INTO t_proj_where VALUES
    ('2024-01-01 00:00:00', 'pageview', 'hello', 100),
    ('2024-01-02 00:00:00', 'click', 'world', 200),
    ('2024-01-03 00:00:00', 'pageview', 'foo', 300),
    ('2024-01-04 00:00:00', 'scroll', 'bar', 400),
    ('2024-01-05 00:00:00', 'pageview', 'baz', 500);

-- Materialize the projection
ALTER TABLE t_proj_where MATERIALIZE PROJECTION proj_pageview;

-- Query with exact matching WHERE.
-- `force_optimize_projection = 1` makes the query throw `PROJECTION_NOT_USED` unless the
-- filtered projection is actually selected, so it proves the projection is used (not the base table).
SELECT 'Exact match';
SELECT time, message FROM t_proj_where
WHERE event_type = 'pageview'
ORDER BY time
SETTINGS force_optimize_projection = 1;

-- Query with stricter WHERE (additional AND condition).
-- Also forced, to verify the stricter-AND implication still selects the projection.
SELECT 'Stricter match (AND)';
SELECT time, message FROM t_proj_where
WHERE event_type = 'pageview' AND time > '2024-01-01 00:00:00'
ORDER BY time
SETTINGS force_optimize_projection = 1;

-- Query with non-matching WHERE
SELECT 'Non-matching (different value)';
SELECT time, message FROM t_proj_where
WHERE event_type = 'click'
ORDER BY time;

-- Query without WHERE
SELECT 'No WHERE';
SELECT count() FROM t_proj_where;

-- Query with weaker WHERE (OR â€” should NOT use projection)
SELECT 'Weaker (OR)';
SELECT time, message FROM t_proj_where
WHERE event_type = 'pageview' OR event_type = 'click'
ORDER BY time;

-- Verify projection stores only filtered rows
SELECT 'Projection row count';
SELECT sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_proj_where' AND name = 'proj_pageview' AND active;

-- Cleanup
DROP TABLE t_proj_where;

-- Projection with `WITH` clause must not produce wrong results.
-- The implication check must be conservative when the projection has a `WITH` clause,
-- because an identifier in projection `WHERE` can reference a CTE/alias that has a
-- different meaning than a same-named table column referenced in the query's `WHERE`.
SELECT 'Projection with WITH clause';

DROP TABLE IF EXISTS t_proj_with;

CREATE TABLE t_proj_with
(
    a UInt8,
    c UInt8
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO t_proj_with VALUES (1, 0), (2, 1), (3, 0);

-- Projection where the identifier `c` in WHERE refers to the WITH-clause expression `(a = 1)`,
-- not the table column `c`.
ALTER TABLE t_proj_with ADD PROJECTION p_with
(
    WITH (a = 1) AS c
    SELECT a, c
    WHERE c
    ORDER BY a
);

ALTER TABLE t_proj_with MATERIALIZE PROJECTION p_with;

-- The query uses the table column `c` in WHERE. Result must reflect the table column,
-- regardless of whether the optimizer considers projection `p_with`.
SELECT a FROM t_proj_with WHERE c ORDER BY a
SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

DROP TABLE t_proj_with;

-- Projection with SELECT alias shadowing must not produce wrong results.
SELECT 'Projection with SELECT alias';

DROP TABLE IF EXISTS t_proj_alias;

CREATE TABLE t_proj_alias
(
    a UInt8,
    c UInt8
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO t_proj_alias VALUES (1, 0), (2, 1), (3, 0);

-- Projection where `c` refers to the SELECT alias expression.
ALTER TABLE t_proj_alias ADD PROJECTION p_alias
(
    SELECT a, (a = 1) AS c
    WHERE c
    ORDER BY a
);

ALTER TABLE t_proj_alias MATERIALIZE PROJECTION p_alias;

-- The query uses the table column `c` in WHERE. Result must reflect the table column.
SELECT a FROM t_proj_alias WHERE c ORDER BY a
SETTINGS force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

DROP TABLE t_proj_alias;

-- A mutation must preserve the projection's WHERE while also applying the mutation.
-- During mutation, the projection part is rematerialized with its WHERE combined with the
-- `_row_exists` condition, so after `ALTER TABLE ... DELETE WHERE` the filtered projection must
-- contain only rows satisfying both the projection predicate and the (negated) delete condition.
SELECT 'Mutation with filtered projection';

DROP TABLE IF EXISTS t_proj_mut;

CREATE TABLE t_proj_mut
(
    time DateTime,
    event_type String,
    message String
)
ENGINE = MergeTree
ORDER BY time;

ALTER TABLE t_proj_mut ADD PROJECTION proj_pageview_mut
(
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

INSERT INTO t_proj_mut VALUES
    ('2024-01-01 00:00:00', 'pageview', 'a'),
    ('2024-01-02 00:00:00', 'click',    'b'),
    ('2024-01-03 00:00:00', 'pageview', 'c'),
    ('2024-01-04 00:00:00', 'pageview', 'd');

ALTER TABLE t_proj_mut MATERIALIZE PROJECTION proj_pageview_mut SETTINGS mutations_sync = 2;

-- Delete one `pageview` row with a heavyweight mutation, which rematerializes the projection part
-- (the projection's WHERE is combined with `_row_exists` during materialization).
ALTER TABLE t_proj_mut DELETE WHERE message = 'a' SETTINGS mutations_sync = 2;

-- `force_optimize_projection = 1` proves the rebuilt projection (not the base table) is read,
-- so the result also confirms the rebuild preserved both the projection predicate and the delete.
SELECT time, message FROM t_proj_mut WHERE event_type = 'pageview' ORDER BY time
SETTINGS force_optimize_projection = 1;

DROP TABLE t_proj_mut;

-- Aggregate projections with WHERE are now supported. The optimizer's predicate implication
-- check ensures a filtered aggregate projection is only selected when the query's WHERE
-- logically implies the projection's WHERE.
SELECT 'Aggregate projection with WHERE';

DROP TABLE IF EXISTS t_proj_agg_where;

CREATE TABLE t_proj_agg_where
(
    k UInt8,
    v UInt64
)
ENGINE = MergeTree
ORDER BY k;

-- Create a filtered aggregate projection: only rows where k = 1 are materialized.
ALTER TABLE t_proj_agg_where ADD PROJECTION p_agg
(
    SELECT k, sum(v)
    WHERE k = 1
    GROUP BY k
);

INSERT INTO t_proj_agg_where VALUES (1, 10), (1, 20), (2, 100), (2, 200), (3, 1000);

ALTER TABLE t_proj_agg_where MATERIALIZE PROJECTION p_agg SETTINGS mutations_sync = 2;

-- Exact match: query WHERE implies projection WHERE → projection should be used.
SELECT 'Aggregate exact match';
SELECT k, sum(v) FROM t_proj_agg_where WHERE k = 1 GROUP BY k
SETTINGS force_optimize_projection = 1;

-- Stricter AND: adding an extra conjunct on the GROUP BY key still implies the projection.
SELECT 'Aggregate stricter AND';
SELECT k, sum(v) FROM t_proj_agg_where WHERE k = 1 AND k < 10 GROUP BY k
SETTINGS force_optimize_projection = 1;

-- Non-matching: query WHERE does not imply projection WHERE → must NOT use projection.
SELECT 'Aggregate non-matching';
SELECT k, sum(v) FROM t_proj_agg_where WHERE k = 2 GROUP BY k;

-- No WHERE: unfiltered aggregation must NOT use the filtered projection.
SELECT 'Aggregate no WHERE';
SELECT k, sum(v) FROM t_proj_agg_where GROUP BY k ORDER BY k;

DROP TABLE t_proj_agg_where;

-- Regression test: filtered aggregate projection where the filter column is NOT part of GROUP BY.
-- The projection materializes only k = 1 rows but does not store k as a key.
-- The residual-filter logic must strip the implied `k = 1` conjunct so that
-- analyzeAggregateProjection does not require k to be computable from projection keys.
SELECT 'Aggregate filter column not in GROUP BY';

DROP TABLE IF EXISTS t_proj_agg_no_key;

CREATE TABLE t_proj_agg_no_key
(
    k UInt8,
    v UInt64
)
ENGINE = MergeTree
ORDER BY k;

ALTER TABLE t_proj_agg_no_key ADD PROJECTION p_agg_no_key
(
    SELECT sum(v)
    WHERE k = 1
);

INSERT INTO t_proj_agg_no_key VALUES (1, 10), (1, 20), (2, 100), (3, 1000);

ALTER TABLE t_proj_agg_no_key MATERIALIZE PROJECTION p_agg_no_key SETTINGS mutations_sync = 2;

-- Exact match: query WHERE matches projection WHERE, k is not in projection keys.
-- The residual filter is empty, so the projection should be usable.
SELECT sum(v) FROM t_proj_agg_no_key WHERE k = 1
SETTINGS force_optimize_projection = 1;

DROP TABLE t_proj_agg_no_key;

-- Regression test: k = 0 (falsy value). When the residual filter is empty, the projection
-- analysis must not misinterpret a key/aggregate column as a filter predicate. With k = 0
-- the key itself is falsy, so treating it as a boolean filter would incorrectly prune the row.
SELECT 'Aggregate exact match k=0';

DROP TABLE IF EXISTS t_proj_agg_k0;

CREATE TABLE t_proj_agg_k0
(
    k UInt8,
    v UInt64
)
ENGINE = MergeTree
ORDER BY k;

ALTER TABLE t_proj_agg_k0 ADD PROJECTION p_agg_k0
(
    SELECT k, sum(v)
    WHERE k = 0
    GROUP BY k
);

INSERT INTO t_proj_agg_k0 VALUES (0, 10), (0, 20), (1, 100);

ALTER TABLE t_proj_agg_k0 MATERIALIZE PROJECTION p_agg_k0 SETTINGS mutations_sync = 2;

SELECT k, sum(v) FROM t_proj_agg_k0 WHERE k = 0 GROUP BY k
SETTINGS force_optimize_projection = 1;

DROP TABLE t_proj_agg_k0;
