-- Lazy materialization replays the expressions that `tryExecuteFunctionsAfterSorting` lifted
-- between `Limit` and `Sort` after the lazy join, i.e. after `LIMIT`/`OFFSET` have already been
-- applied and after the join changed the block boundaries. That is only sound for expressions
-- whose value does not depend on which rows and blocks they see, so a lifted expression with a
-- stateful function such as `rowNumberInAllBlocks`, or a function that is not deterministic in
-- the scope of a query such as `blockNumber`, must not be replayed there.

-- The absolute values of `rowNumberInAllBlocks` in the reference depend on how rows are numbered
-- on a single replica, so pin `enable_parallel_replicas = 0` (under parallel replicas the rows
-- are numbered differently, identically with and without the optimization).
SET enable_analyzer = 1, query_plan_optimize_lazy_materialization = true, query_plan_max_limit_for_lazy_materialization = 100, max_threads = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS test_lazy_alias_stateful SYNC;
CREATE TABLE test_lazy_alias_stateful
(
    time       DateTime64(3),
    body       String,
    body_alias String ALIAS if(length(body) > 5, 'long', 'short')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 100;

INSERT INTO test_lazy_alias_stateful
SELECT
    toDateTime64('2020-01-01 00:00:00', 3) - number AS time,
    repeat('x', number % 20) AS body
FROM numbers(1000);

-- The plan assertion pins `pretty = 0` to get the legacy indented output, because `trimLeft` does
-- not strip the box-drawing prefixes of the default pretty plan rendering.

-- 1. Baseline: an `ALIAS` column still gets the optimization, also with `OFFSET`.
SELECT 'plain_alias_plan';
SELECT trimLeft(explain) AS s
FROM (EXPLAIN pretty = 0 SELECT body_alias FROM test_lazy_alias_stateful ORDER BY time DESC LIMIT 10 OFFSET 5)
WHERE s LIKE 'LazilyRead%';

-- 2. A stateful function must produce the same values with and without the optimization.
SELECT 'stateful_result';
SELECT rowNumberInAllBlocks() AS n, body_alias
FROM test_lazy_alias_stateful ORDER BY time DESC LIMIT 10 OFFSET 5;

SELECT 'stateful_result_without_optimization';
SELECT rowNumberInAllBlocks() AS n, body_alias
FROM test_lazy_alias_stateful ORDER BY time DESC LIMIT 10 OFFSET 5
SETTINGS query_plan_optimize_lazy_materialization = false;

DROP TABLE test_lazy_alias_stateful;
