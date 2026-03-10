-- Tags: no-parallel-replicas

-- Test that runtime filter exact set enables granule-level pruning via primary key index.
-- When the right side of a hash join has few distinct key values, the runtime filter's exact set
-- allows skipping granules on the left side that cannot contain matching rows.

DROP TABLE IF EXISTS big;
DROP TABLE IF EXISTS small;

-- Create a table with many rows, ordered by id.
-- With default index_granularity=8192, this creates multiple granules.
CREATE TABLE big (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO big SELECT number, 'value_' || toString(number) FROM numbers(100000);

-- Create a small table with just a few rows whose id values fall into a narrow range.
CREATE TABLE small (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO small VALUES (42, 'a'), (43, 'b'), (44, 'c');

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';

-- Verify the join returns correct results with runtime filters enabled
SELECT 'correctness';
SELECT id, name FROM big INNER JOIN small USING (id)
ORDER BY id
SETTINGS enable_join_runtime_filters = 1;

-- Verify the runtime filter is created and the __applyFilter condition is applied
SELECT 'explain_check';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_ID')
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', 'actions = 1', (
        SELECT count()
        FROM big INNER JOIN small USING (id)
        SETTINGS enable_join_runtime_filters = 1, optimize_move_to_prewhere = 1
    ))
)
WHERE (explain LIKE '%ReadFromMergeTree%big%') OR (explain LIKE '%__applyFilter%') OR (explain LIKE '%Build%');

DROP TABLE big;
DROP TABLE small;
