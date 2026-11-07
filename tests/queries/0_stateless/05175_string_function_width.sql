-- `concat(materialize(repeat('x', 1000)), s)` changes string length: discard the input width.
-- `identity(s)` keeps strings unchanged: keep the input width.
-- `assumeNotNull(s)` removes Nullable without changing the string bytes: keep the input width.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_cascades_optimizer = 0;
SET make_distributed_plan = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;

-- Pin row counts, column widths, and part layout for the width-dependent plan checks.
-- The join matches 5 of the 10 `g` values, so its estimated output is 5M rows.
SET max_threads = 4;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"fact": {"cardinality": 10000000, "avg_row_bytes": 24, "distinct_keys": {"k": 1000000, "g": 10}, "column_bytes": {"k": 8, "g": 8}}, "dim": {"cardinality": 5, "avg_row_bytes": 18, "distinct_keys": {"g": 5}, "column_bytes": {"g": 8, "s": 10}}}';

CREATE TABLE fact (k UInt64, g UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS auto_statistics_types = '', index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE dim (g UInt64, s String) ENGINE = MergeTree ORDER BY g
    SETTINGS auto_statistics_types = '', index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO fact SELECT number, number % 10, number FROM numbers(1000) SETTINGS max_insert_threads = 1;
INSERT INTO dim SELECT number, leftPad(toString(number), 10, 'x') FROM numbers(5) SETTINGS max_insert_threads = 1;

-- Check that `concat(materialize(repeat('x', 1000)), s)` discards the 10-byte input width as strings grow to 1010 bytes.
-- The 64-byte `String` default makes shuffling the 10M fact rows cheaper than shuffling the 5M joined rows.
-- Expect `ShuffleExchange (rows: ~10000000.0)`.
SELECT 'width: concat(materialize(repeat(\'x\', 1000)), s)';
SELECT concat(extract(explain, 'ShuffleExchange[^,]+'), ')') FROM
(
    EXPLAIN estimates = 1
    SELECT k, sum(v), any(s)
    FROM fact AS f
    JOIN (SELECT g, concat(materialize(repeat('x', 1000)), s) AS s FROM dim) AS d ON f.g = d.g
    GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
             distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
)
WHERE explain LIKE '%ShuffleExchange%';

-- Check that `identity(s)` retains the 10-byte width because strings are unchanged.
-- Shuffling the 5M joined rows is then cheaper than shuffling the 10M fact rows.
-- Expect `ShuffleExchange (rows: ~5000000.0)`.
SELECT 'width: identity(s)';
SELECT concat(extract(explain, 'ShuffleExchange[^,]+'), ')') FROM
(
    EXPLAIN estimates = 1
    SELECT k, sum(v), any(s)
    FROM fact AS f
    JOIN (SELECT g, identity(s) AS s FROM dim) AS d ON f.g = d.g
    GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
             distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
)
WHERE explain LIKE '%ShuffleExchange%';

ALTER TABLE dim MODIFY COLUMN s Nullable(String);

-- `assumeNotNull(s)` retains the 10-byte string width after removing Nullable.
-- Before the fix, the 64-byte default caused a shuffle of 10M rows; expect the 5M-row shuffle instead.
SELECT 'width: assumeNotNull(s)';
SELECT concat(extract(explain, 'ShuffleExchange[^,]+'), ')') FROM
(
    EXPLAIN estimates = 1
    SELECT k, sum(v), any(s)
    FROM fact AS f
    JOIN (SELECT g, assumeNotNull(s) AS s FROM dim) AS d ON f.g = d.g
    GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1,
             distributed_plan_force_shuffle_aggregation = 1, enable_join_runtime_filters = 0
)
WHERE explain LIKE '%ShuffleExchange%';

DROP TABLE fact;
DROP TABLE dim;
