-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A sorted `GatherExchange` under a step whose `ActionsDAG` does not mention a sort column at all.
-- `ActionsDAG::updateHeader` copies such a column from the input header into the output header without
-- giving it a node in `outputs`, so the move-up check found it in the header and then failed to look it
-- up in the DAG. Before the fix every query below was rejected during plan optimization with
-- `Unknown identifier`.
--
-- The shape needs two ingredients at once: a column-keyed inner window supplies the sorted gather, and
-- an outer window keyed by a constant supplies the step above it whose DAG holds only that constant, so
-- every other header column is a pass-through. Either alone does not reach the branch.

DROP TABLE IF EXISTS t_passthrough_sort;

-- The wrapped columns cover the sort column's type matrix: the header carries the column whatever its
-- type, so all of these reach the same branch.
CREATE TABLE t_passthrough_sort
(
    a UInt32,
    a_nullable Nullable(UInt32),
    a_low_cardinality LowCardinality(String),
    a_low_cardinality_nullable LowCardinality(Nullable(String)),
    a_array Array(UInt32),
    v UInt32
)
ENGINE = MergeTree ORDER BY (a, v) SETTINGS index_granularity = 256, allow_nullable_key = 1;

-- Two parts so the fragment below the gather reads with more than one stream.
SYSTEM STOP MERGES t_passthrough_sort;
INSERT INTO t_passthrough_sort SELECT number % 10, if(number % 13 = 0, NULL, number % 10),
    toString(number % 7), if(number % 11 = 0, NULL, toString(number % 7)), [number % 3], number
FROM numbers(4000);
INSERT INTO t_passthrough_sort SELECT number % 10, if(number % 13 = 0, NULL, number % 10),
    toString(number % 7), if(number % 11 = 0, NULL, toString(number % 7)), [number % 3], number + 10000000
FROM numbers(4000);

-- max_rows_to_group_by must be 0, otherwise make_distributed_plan declines plans with an aggregation.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_default_reader_bucket_count = 2,
    distributed_plan_optimize_exchanges = 1, max_threads = 4, max_rows_to_group_by = 0,
    optimize_read_in_order = 0, optimize_sorting_by_input_stream_properties = 1;

-- The outer window argument is a non-trivial expression on purpose: a plain aggregate is pushed below
-- the gather and does not build the failing shape.
SELECT 'query runs', count() FROM
(
    SELECT uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
    FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_passthrough_sort)
);

-- The same shape with the sort column wrapped. Each of these was rejected before the fix too.
SELECT 'Nullable key', count() FROM
(
    SELECT uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
    FROM (SELECT a_nullable, v, sum(v) OVER (PARTITION BY a_nullable ORDER BY v) AS s FROM t_passthrough_sort)
);

SELECT 'LowCardinality key', count() FROM
(
    SELECT uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
    FROM (SELECT a_low_cardinality, v, sum(v) OVER (PARTITION BY a_low_cardinality ORDER BY v) AS s
        FROM t_passthrough_sort)
);

SELECT 'LowCardinality(Nullable) key', count() FROM
(
    SELECT uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
    FROM (SELECT a_low_cardinality_nullable, v,
        sum(v) OVER (PARTITION BY a_low_cardinality_nullable ORDER BY v) AS s FROM t_passthrough_sort)
);

SELECT 'Array key', count() FROM
(
    SELECT uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
    FROM (SELECT a_array, v, sum(v) OVER (PARTITION BY a_array ORDER BY v) AS s FROM t_passthrough_sort)
);

-- An empty input still builds the plan, so the branch is reached with no rows to merge.
SELECT 'empty input', count() FROM
(
    SELECT uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
    FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_passthrough_sort
        WHERE v > 999999999)
);

-- The move-up witness. A `GatherExchange` is present in this plan whether or not it moved
-- (`tryMakeDistributedSorting` creates it unconditionally), so its mere presence proves nothing. What
-- the fix controls is the swap: once the gather moves above the pass-through step it is replaced by a
-- keyed shuffle, and the `(a ASC, v ASC)` sorted gather disappears. Answering "not preserved" instead
-- would keep that gather in place, so this arm distinguishes the two candidate answers.
-- `make_distributed_plan` is repeated inside because the outer `SETTINGS` propagates into the subquery
-- and the outer query must stay local.
SELECT 'inner sorted gather moved up', count() = 0 FROM
(
    EXPLAIN SELECT count() FROM
    (
        SELECT uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
            OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
        FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_passthrough_sort)
    ) SETTINGS make_distributed_plan = 1
)
WHERE explain ILIKE '%GatherExchange (sorted by (a ASC, v ASC))%'
SETTINGS make_distributed_plan = 0;

-- Control for the arm above: with the exchange rewrite off no move-up may happen, so the sorted
-- gather must still be there. Without this the arm could pass for a planner that stopped distributing.
SELECT 'rewrite off keeps the gather below', count() > 0 FROM
(
    EXPLAIN SELECT count() FROM
    (
        SELECT uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
            OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
        FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_passthrough_sort)
    ) SETTINGS make_distributed_plan = 1, distributed_plan_optimize_exchanges = 0
)
WHERE explain ILIKE '%GatherExchange (sorted by (a ASC, v ASC))%'
SETTINGS make_distributed_plan = 0;

-- The distributed result for the branch-reaching shape, and the same query planned locally. The local
-- reference needs its own statement: optimization is whole-statement, so a FROM-subquery `SETTINGS` does
-- not exempt its subtree. Measured equal for both candidate answers: regression coverage, not a witness.
SELECT 'window values distributed', sum(cityHash64(a, v, s, r)) FROM
(
    SELECT a, v, s, uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
    FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_passthrough_sort)
);

SELECT 'window values local', sum(cityHash64(a, v, s, r)) FROM
(
    SELECT a, v, s, uniq(modulo(s, finalizeAggregation(initializeAggregation('anyState', toNullable(-1)))))
        OVER (PARTITION BY 'c' ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS r
    FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_passthrough_sort)
) SETTINGS make_distributed_plan = 0;

DROP TABLE t_passthrough_sort;
