-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

-- The row wrapper rewrite replaces the wrapped columns in the reading step's column list with
-- the wrapper column, and projection matching requires every name in that list to exist in the
-- projection part. So the rewrite must run only after projection selection - otherwise
-- `query_plan_use_row_wrappers = 1` would disable an otherwise usable projection.

DROP TABLE IF EXISTS row_wrapper_projection;

-- A projection is chosen only if it reads fewer marks than the table. With a randomized granularity
-- of tens of thousands of rows both read the same couple of marks, so the granularity is pinned.
CREATE TABLE row_wrapper_projection (
    id UInt64,
    a UInt64,
    b UInt64,
    c UInt64,
    combined Row(a UInt64, b UInt64, c UInt64) MATERIALIZED tuple(a, b, c),
    PROJECTION p (SELECT a, b, c ORDER BY a)
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;

INSERT INTO row_wrapper_projection (id, a, b, c)
    SELECT number, number % 100, number * 2, number * 3 FROM numbers(100000);

SELECT countIf(explain LIKE '%ReadFromMergeTree (p)%') FROM (
    EXPLAIN SELECT b, c FROM row_wrapper_projection WHERE a = 42
    SETTINGS query_plan_use_row_wrappers = 1, optimize_use_projections = 1, enable_parallel_replicas = 0
);

SELECT sum(b), sum(c) FROM row_wrapper_projection WHERE a = 42
    SETTINGS query_plan_use_row_wrappers = 1, optimize_use_projections = 1, force_optimize_projection = 1,
             enable_parallel_replicas = 0;

SELECT sum(b), sum(c) FROM row_wrapper_projection WHERE a = 42
    SETTINGS query_plan_use_row_wrappers = 0, optimize_use_projections = 0;

-- With no projection to lose, the rewrite still applies.
SELECT countIf(explain LIKE '%__rowElement%') > 0 FROM (
    EXPLAIN actions = 1 SELECT a, b, c FROM row_wrapper_projection
    SETTINGS query_plan_use_row_wrappers = 1, optimize_use_projections = 0, enable_parallel_replicas = 0
);

DROP TABLE row_wrapper_projection;
