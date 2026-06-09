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

-- Test 1: Query with exact matching WHERE.
-- `force_optimize_projection = 1` makes the query throw `PROJECTION_NOT_USED` unless the
-- filtered projection is actually selected, so it proves the projection is used (not the base table).
SELECT 'Test 1: Exact match';
SELECT time, message FROM t_proj_where
WHERE event_type = 'pageview'
ORDER BY time
SETTINGS force_optimize_projection = 1;

-- Test 2: Query with stricter WHERE (additional AND condition).
-- Also forced, to verify the stricter-AND implication still selects the projection.
SELECT 'Test 2: Stricter match (AND)';
SELECT time, message FROM t_proj_where
WHERE event_type = 'pageview' AND time > '2024-01-01 00:00:00'
ORDER BY time
SETTINGS force_optimize_projection = 1;

-- Test 3: Query with non-matching WHERE
SELECT 'Test 3: Non-matching (different value)';
SELECT time, message FROM t_proj_where
WHERE event_type = 'click'
ORDER BY time;

-- Test 4: Query without WHERE
SELECT 'Test 4: No WHERE';
SELECT count() FROM t_proj_where;

-- Test 5: Query with weaker WHERE (OR — should NOT use projection)
SELECT 'Test 5: Weaker (OR)';
SELECT time, message FROM t_proj_where
WHERE event_type = 'pageview' OR event_type = 'click'
ORDER BY time;

-- Test 6: Verify projection stores only filtered rows
SELECT 'Test 6: Projection row count';
SELECT count() FROM t_proj_where WHERE event_type = 'pageview';

-- Cleanup
DROP TABLE t_proj_where;

-- Test 7: Projection with `WITH` clause must not produce wrong results.
-- The implication check must be conservative when the projection has a `WITH` clause,
-- because an identifier in projection `WHERE` can reference a CTE/alias that has a
-- different meaning than a same-named table column referenced in the query's `WHERE`.
SELECT 'Test 7: Projection with WITH clause';

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
SELECT a FROM t_proj_with WHERE c ORDER BY a;

DROP TABLE t_proj_with;

-- Test 8: A mutation must preserve the projection's WHERE while also applying the mutation.
-- During mutation, the projection part is rematerialized with its WHERE combined with the
-- `_row_exists` condition, so after `ALTER TABLE ... DELETE WHERE` the filtered projection must
-- contain only rows satisfying both the projection predicate and the (negated) delete condition.
SELECT 'Test 8: Mutation with filtered projection';

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

-- Test 9: A WHERE clause is not supported for aggregate projections and must be rejected at creation.
-- Otherwise a filtered aggregate projection could be matched for an unfiltered aggregation
-- (e.g. `SELECT k, sum(v) GROUP BY k`) and silently return results over only the filtered subset.
SELECT 'Test 9: Aggregate projection with WHERE is rejected';

DROP TABLE IF EXISTS t_proj_agg_where;

CREATE TABLE t_proj_agg_where
(
    k UInt8,
    v UInt64
)
ENGINE = MergeTree
ORDER BY k;

ALTER TABLE t_proj_agg_where ADD PROJECTION p_agg
(
    SELECT k, sum(v)
    WHERE k = 1
    GROUP BY k
); -- { serverError ILLEGAL_PROJECTION }

DROP TABLE t_proj_agg_where;
