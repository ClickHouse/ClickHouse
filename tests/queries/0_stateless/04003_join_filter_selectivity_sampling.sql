-- Tags: no-fasttest, no-parallel-replicas
-- Test that filter selectivity sampling improves join ordering decisions.
-- When a highly selective filter is applied to one side of a join,
-- the optimizer should use granule sampling to estimate the filtered row count
-- and choose the filtered side as the build table.

DROP TABLE IF EXISTS big_table;
DROP TABLE IF EXISTS small_table;

SET enable_analyzer = 1;

-- Big table: 500k rows, filter on `category` will select ~1% of rows (~5000).
CREATE TABLE big_table (
    id UInt64,
    category UInt8,
    value String
) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', auto_statistics_types = '';

INSERT INTO big_table SELECT number, number % 100, toString(number) FROM numbers(500_000);

-- Small table: 50k rows, no filter.
CREATE TABLE small_table (
    id UInt64,
    name String
) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '';

INSERT INTO small_table SELECT number, 'name_' || toString(number) FROM numbers(50_000);

SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_limit = 2;
SET use_statistics = 0;
SET enable_join_runtime_filters = 0;

-- The key test case is the `big_table, small_table` order.
-- Default behavior (no swap): small_table (right) is the build table (50k rows).
-- With sampling: the optimizer detects big_table has only ~5000 rows after filter,
-- so it swaps to make big_table the build table (5000 < 50000).
-- Without sampling: the optimizer can't estimate the filter effect, so it keeps
-- the default order with small_table as the build table (50k rows).

SELECT * FROM big_table, small_table
WHERE big_table.id = small_table.id AND big_table.category = 1
SETTINGS
    log_comment = '04003_sampling_enabled',
    query_plan_estimate_filter_selectivity_by_sampling = 1
FORMAT Null;

SELECT * FROM big_table, small_table
WHERE big_table.id = small_table.id AND big_table.category = 1
SETTINGS
    log_comment = '04003_sampling_disabled',
    query_plan_estimate_filter_selectivity_by_sampling = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- With sampling enabled: the optimizer swaps to make the filtered big_table
-- the build table (~5000 rows after filter).

SELECT
    'sampling_enabled',
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 3000 AND 7000, 'ok',
       format('fail({}): build={} probe={}', query_id,
              ProfileEvents['JoinBuildTableRowCount'],
              ProfileEvents['JoinProbeTableRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select'
    AND current_database = currentDatabase()
    AND query LIKE '%big_table, small_table%'
    AND log_comment = '04003_sampling_enabled'
ORDER BY event_time DESC
LIMIT 1;

-- With sampling disabled: the optimizer cannot estimate the filter effect,
-- so it keeps the default order with small_table as the build table (~50k rows).

SELECT
    'sampling_disabled',
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 40000 AND 60000, 'ok',
       format('fail({}): build={} probe={}', query_id,
              ProfileEvents['JoinBuildTableRowCount'],
              ProfileEvents['JoinProbeTableRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select'
    AND current_database = currentDatabase()
    AND query LIKE '%big_table, small_table%'
    AND log_comment = '04003_sampling_disabled'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS big_table;
DROP TABLE IF EXISTS small_table;
