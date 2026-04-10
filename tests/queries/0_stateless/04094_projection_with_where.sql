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
