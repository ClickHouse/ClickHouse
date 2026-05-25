-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echo }

SET query_plan_push_limit_by_into_sort = 1;
SET max_threads = 12;

-- `LIMIT BY` keys are not a prefix of the sort keys, so the optimization is not applied.
-- The pipeline keeps only the final `LimitByTransform` above the sort.
EXPLAIN PIPELINE
SELECT number % 100 AS k, number AS v
FROM numbers_mt(1000000)
ORDER BY v LIMIT 2, 3 BY k;

-- `LIMIT BY` keys are a prefix of the sort keys, so the optimization adds
-- per-stream `LimitByTransform`s before sorted streams are merged.
-- The query plan shape stays unchanged; only the `SortingStep` metadata changes.
EXPLAIN PLAN
SELECT number % 100 AS k, number AS v
FROM numbers_mt(1000000)
ORDER BY k, v LIMIT 2, 3 BY k;

-- Text `EXPLAIN` includes the per-stream `LIMIT BY` hint attached to `SortingStep`.
EXPLAIN actions=1
SELECT number % 100 AS k, number AS v
FROM numbers_mt(1000000)
ORDER BY k, v LIMIT 2, 3 BY k;

-- JSON `EXPLAIN` includes the same per-stream `LIMIT BY` hint.
SELECT trimLeft(line)
FROM
(
    SELECT arrayJoin(splitByChar('\n', explain)) AS line
    FROM
    (
        EXPLAIN json=1, actions=1
        SELECT number FROM numbers(1000)
        ORDER BY number LIMIT 2, 3 BY number
    )
)
WHERE line LIKE '%Per-stream LIMIT BY%'
FORMAT TSVRaw;

EXPLAIN PIPELINE
SELECT number % 100 AS k, number AS v
FROM numbers_mt(1000000)
ORDER BY k, v LIMIT 2, 3 BY k;

-- A derived `LIMIT BY` expression that is not a sort-key prefix is not optimized.
EXPLAIN PIPELINE
SELECT number % 100 AS grp, number AS v
FROM numbers_mt(1000000)
ORDER BY v LIMIT 2, 3 BY grp % 2;

DROP TABLE IF EXISTS test_mt;
CREATE TABLE test_mt (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO test_mt SELECT number % 100, number FROM numbers(10000);
INSERT INTO test_mt SELECT number % 100, number FROM numbers(10000);

-- Read-in-order input already satisfies the full sort order, so the optimization is applied
-- before sorted streams are merged.
EXPLAIN PIPELINE
SELECT a, b FROM test_mt ORDER BY a, b LIMIT 2, 3 BY a;

-- `FinishSorting` with an extra sort suffix is not optimized before the final order is
-- complete, because suffix sort keys can change which rows survive `LIMIT BY`.
EXPLAIN PIPELINE
SELECT a, b FROM test_mt ORDER BY a, b, b + 1 LIMIT 2, 3 BY a;

-- Queries without `ORDER BY` have no `SortingStep` to receive the per-stream hint.
EXPLAIN PIPELINE
SELECT number % 1000 AS k FROM numbers_mt(1000000) LIMIT 2, 3 BY k;

-- The optimization can be disabled explicitly.
EXPLAIN PIPELINE
SELECT number % 100 AS k, number AS v
FROM numbers_mt(1000000)
ORDER BY k, v LIMIT 2, 3 BY k
SETTINGS query_plan_push_limit_by_into_sort = 0;

-- The optimization is not applied when `LIMIT BY` offset plus length overflows `UInt64`.
EXPLAIN PIPELINE
SELECT number % 1000 AS k FROM numbers_mt(1000000)
ORDER BY k LIMIT 18446744073709551615, 10 BY k;

-- A sort above `UNION ALL` can receive the per-stream `LIMIT BY` hint.
EXPLAIN PIPELINE
SELECT k, v FROM (
    SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
    UNION ALL
    SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
)
ORDER BY k, v LIMIT 2, 3 BY k;

-- A `LIMIT BY` inside one `UNION ALL` branch is optimized only inside that branch.
EXPLAIN PIPELINE
SELECT k, v FROM (
    SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
    ORDER BY k, v LIMIT 2, 3 BY k
    UNION ALL
    SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
);

-- A sort above `JOIN` can receive the per-stream `LIMIT BY` hint.
DROP TABLE IF EXISTS test_join_a;
DROP TABLE IF EXISTS test_join_b;
CREATE TABLE test_join_a (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE test_join_b (k UInt32, w UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_join_a SELECT number % 100, number FROM numbers(10000);
INSERT INTO test_join_a SELECT number % 100, number FROM numbers(10000);
INSERT INTO test_join_b SELECT number % 100, number FROM numbers(10000);
INSERT INTO test_join_b SELECT number % 100, number FROM numbers(10000);

EXPLAIN PIPELINE
SELECT a.k AS k, a.v AS v
FROM test_join_a a JOIN test_join_b b ON a.k = b.k
ORDER BY k, v LIMIT 2, 3 BY k;

-- With `full_sorting_merge`, the optimization applies to the final `ORDER BY` sort above
-- the `JOIN`, not to the input sorts used by the join algorithm.
EXPLAIN PIPELINE
SELECT a.k AS k, a.v AS v
FROM test_join_a a JOIN test_join_b b ON a.k = b.k
ORDER BY k, v LIMIT 2, 3 BY k
SETTINGS join_algorithm = 'full_sorting_merge';

DROP TABLE test_join_a;
DROP TABLE test_join_b;

-- The optimization is not applied when a `WindowStep` separates `LimitByStep` from
-- `SortingStep`.
EXPLAIN PIPELINE
SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn
FROM (
    SELECT number % 100 AS k, number AS v FROM numbers_mt(1000000)
    ORDER BY k, v
)
LIMIT 2, 3 BY k;

DROP TABLE test_mt;
