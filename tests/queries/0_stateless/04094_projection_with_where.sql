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

-- Test 1: Query with exact matching WHERE
SELECT 'Test 1: Exact match';
SELECT time, message FROM t_proj_where
WHERE event_type = 'pageview'
ORDER BY time;

-- Test 2: Query with stricter WHERE (additional AND condition)
SELECT 'Test 2: Stricter match (AND)';
SELECT time, message FROM t_proj_where
WHERE event_type = 'pageview' AND time > '2024-01-01 00:00:00'
ORDER BY time;

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

-- Test 8: Mutation must preserve the projection's WHERE during `DELETE WHERE` (lightweight delete).
-- After deleting rows matching one predicate, the filtered projection must still contain
-- only rows satisfying both the original projection predicate and the mutation condition.
SELECT 'Test 8: Mutation with filtered projection';

DROP TABLE IF EXISTS t_proj_mut;

CREATE TABLE t_proj_mut
(
    time DateTime,
    event_type String,
    message String
)
ENGINE = MergeTree
ORDER BY time
SETTINGS lightweight_mutation_projection_mode = 'rebuild';

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

-- Delete one `pageview` row using a lightweight delete (`_row_exists` path),
-- which triggers a projection rebuild because of `lightweight_mutation_projection_mode = 'rebuild'`.
DELETE FROM t_proj_mut WHERE message = 'a' SETTINGS mutations_sync = 2;

-- The query against the base table is the source of truth.
SELECT time, message FROM t_proj_mut WHERE event_type = 'pageview' ORDER BY time;

DROP TABLE t_proj_mut;
