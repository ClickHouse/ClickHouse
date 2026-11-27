-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A window `PARTITION BY <constant>` under make_distributed_plan=1 builds a `GatherSend` fragment whose
-- sort description is entirely constant. Such a description orders nothing, so the fragment's
-- order-preserving merge must not be used: it waits for every input stream to have data, which never
-- completes while the streams share one blocked upstream scatter. Before the fix every query below with
-- a constant partition key deadlocked until `receive_timeout` and then raised `Pipeline stuck`.

DROP TABLE IF EXISTS t_const_window;

CREATE TABLE t_const_window (a UInt32, v UInt32)
ENGINE = MergeTree ORDER BY (a, v) SETTINGS index_granularity = 256;

-- Several parts so the fragment reads with more than one stream and the scatter actually fans out.
SYSTEM STOP MERGES t_const_window;
INSERT INTO t_const_window SELECT number % 10, number FROM numbers(4000);
INSERT INTO t_const_window SELECT number % 10, number + 10000000 FROM numbers(4000);
INSERT INTO t_const_window SELECT number % 10, number + 20000000 FROM numbers(4000);
INSERT INTO t_const_window SELECT number % 10, number + 30000000 FROM numbers(4000);
INSERT INTO t_const_window SELECT number % 10, number + 40000000 FROM numbers(4000);

-- Two buckets with four threads keeps several streams inside one fragment, which is what puts the
-- merge and the scatter in the deadlocking state. max_rows_to_group_by must be 0, otherwise
-- make_distributed_plan declines plans with an aggregation.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_default_reader_bucket_count = 2,
    distributed_plan_optimize_exchanges = 1, max_threads = 4, max_rows_to_group_by = 0,
    optimize_read_in_order = 0, optimize_sorting_by_input_stream_properties = 1, max_block_size = 1024;

-- The window argument is a non-trivial expression on purpose: it keeps the window above the gather, so
-- the fragment below the gather is the const-keyed sort. A plain `uniq(v)` is pushed below the gather
-- instead and does not build the failing shape.
SELECT 'constant key', count() FROM
(
    SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window
);

-- A constant partition key must build a gather whose sort description is nothing but that constant.
-- `make_distributed_plan` is repeated inside because the outer `SETTINGS` propagates into the subquery,
-- and the outer query must stay local: distributed it fails with `ReadFromStorage is not serializable`.
SELECT 'const gather is const-sorted', count() > 0 FROM
(
    EXPLAIN SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window SETTINGS make_distributed_plan = 1
)
WHERE explain ILIKE '%GatherExchange (sorted by (''c''_String ASC))%'
SETTINGS make_distributed_plan = 0;

-- Constants wrapped in LowCardinality and Nullable reach the same shape.
SELECT 'constant key, LowCardinality', count() FROM
(
    SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY toLowCardinality('c') ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window
);

SELECT 'constant key, Nullable', count() FROM
(
    SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY toNullable('c') ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window
);

-- A function that returns its argument column verbatim keeps the key constant, so the description is
-- still all-constant after the const-strip. `materialize` is the opposite case: it converts to a full
-- column, the strip keeps the key, and the sort really sorts, so that shape never reached the merge.
SELECT 'constant key behind identity', count() FROM
(
    SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY identity('c') ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window
);

SELECT 'materialized key is not constant', count() FROM
(
    SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY materialize('c') ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window
);

-- Controls. A real column key, and a mixed key whose second component is a column, both order rows and
-- must keep using the merge; they passed before the fix and must keep passing.
SELECT 'column key', count() FROM
(
    SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY a ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window
);

-- A column partition key keeps a column in the gather's sort description, so this arm and the constant
-- one above are distinguishable rather than both merely returning the same row count.
SELECT 'column gather is column-sorted', count() > 0 FROM
(
    EXPLAIN SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY a ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window SETTINGS make_distributed_plan = 1
)
WHERE explain ILIKE '%GatherExchange (sorted by (a ASC))%'
SETTINGS make_distributed_plan = 0;

SELECT 'mixed key', count() FROM
(
    SELECT uniq(modulo(v, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c', a ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS roll
    FROM t_const_window
);

-- A constant partition key is one partition, so the distributed and local results must agree.
SELECT 'values match local', d.h = l.h FROM
(
    SELECT sum(cityHash64(a, v, roll)) AS h FROM
    (
        SELECT a, v, sum(v) OVER (PARTITION BY 'c' ORDER BY a, v
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS roll
        FROM t_const_window
    )
) AS d,
(
    SELECT sum(cityHash64(a, v, roll)) AS h FROM
    (
        SELECT a, v, sum(v) OVER (PARTITION BY 'c' ORDER BY a, v
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS roll
        FROM t_const_window
    ) SETTINGS make_distributed_plan = 0
) AS l;

DROP TABLE t_const_window;
