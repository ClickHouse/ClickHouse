-- Tags: no-parallel-replicas
-- Bounded streaming reads everything committed so far and finishes, so it runs synchronously.

SET enable_analyzer = 1; -- streaming queries require the analyzer (CI randomizes this setting)
SET enable_streaming_queries = 1;
-- A projection makes an earlier optimizer memoize the analysis result, which is the state in
-- which the range set captured for a lazy re-read does not match what a STREAM read selects.
SET optimize_use_projections = 1;

DROP TABLE IF EXISTS t_streaming_lazy_mat;

CREATE TABLE t_streaming_lazy_mat
(id UInt64, region String, value UInt64, PROJECTION region_proj INDEX region TYPE basic)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16, min_bytes_for_wide_part = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_streaming_lazy_mat
SELECT number, if(number BETWEEN 1600 AND 1610, 'rare', 'common'), number * 10 FROM numbers(4096);

-- `value` is neither sorted nor filtered on, so it is the column lazy materialization defers.
-- The limit cap is pinned to its default because a lower value declines the optimization before
-- the streaming check is reached, which would leave the query below exercising nothing.
SELECT id, value, region FROM t_streaming_lazy_mat STREAM BOUNDED
WHERE region = 'rare' ORDER BY id ASC LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000,
    max_threads = 1, optimize_read_in_order = 1;

-- Control: the optimization must be transparent here, so the same rows with it off.
SELECT id, value, region FROM t_streaming_lazy_mat STREAM BOUNDED
WHERE region = 'rare' ORDER BY id ASC LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0, query_plan_max_limit_for_lazy_materialization = 10000,
    max_threads = 1, optimize_read_in_order = 1;

SELECT 'stream lazy steps', count() FROM (EXPLAIN actions = 1
    SELECT id, value, region FROM t_streaming_lazy_mat STREAM BOUNDED
    WHERE region = 'rare' ORDER BY id ASC LIMIT 5
    SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000,
        max_threads = 1, optimize_read_in_order = 1)
WHERE explain ILIKE '%LazilyReadFromMergeTree%';

SELECT 'non-stream lazy steps', count() > 0 FROM (EXPLAIN actions = 1
    SELECT id, value, region FROM t_streaming_lazy_mat
    WHERE region = 'rare' ORDER BY id ASC LIMIT 5
    SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000,
        max_threads = 1, optimize_read_in_order = 1)
WHERE explain ILIKE '%LazilyReadFromMergeTree%';

DROP TABLE t_streaming_lazy_mat;
