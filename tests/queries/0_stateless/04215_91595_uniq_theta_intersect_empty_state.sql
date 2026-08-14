-- Tags: no-fasttest
-- ^ no-fasttest because uniqTheta requires the DataSketches library

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/91595
-- `uniqThetaIntersect` previously returned the cardinality of the first
-- argument when the second argument was a freshly-created (never-updated)
-- state. Such states arise naturally from
-- `uniqThetaMergeStateIf(s, predicate)` when the predicate excludes every
-- row, and the intersection with an empty set must always be empty.

SELECT 'reproducer from den-crane';
SELECT finalizeAggregation(
    uniqThetaIntersect(
        uniqThetaMergeStateIf(arrayReduce('uniqThetaState', [1, 2]::Array(UInt32)), 1),
        uniqThetaMergeStateIf(arrayReduce('uniqThetaState', []::Array(UInt32)), 0)
    )
);

SELECT 'intersect non_empty with filter-excluded empty state';
SELECT finalizeAggregation(
    uniqThetaIntersect(
        uniqThetaStateIf(number, 1),
        uniqThetaStateIf(number, 0)
    )
)
FROM numbers(10);

SELECT 'intersect filter-excluded empty state with non_empty (commuted)';
SELECT finalizeAggregation(
    uniqThetaIntersect(
        uniqThetaStateIf(number, 0),
        uniqThetaStateIf(number, 1)
    )
)
FROM numbers(10);

SELECT 'intersect two filter-excluded empty states';
SELECT finalizeAggregation(
    uniqThetaIntersect(
        uniqThetaStateIf(number, 0),
        uniqThetaStateIf(number, 0)
    )
)
FROM numbers(10);

SELECT 'sanity check - non-empty intersection still works';
SELECT finalizeAggregation(
    uniqThetaIntersect(
        uniqThetaState(number),
        uniqThetaStateIf(number, number < 5)
    )
)
FROM numbers(10);

SELECT 'AggregatingMergeTree variant from the issue';
DROP TABLE IF EXISTS test_uniq_theta_intersect_91595;

CREATE TABLE test_uniq_theta_intersect_91595
(
    cid UInt32,
    users AggregateFunction(uniqTheta, UInt32)
)
ENGINE = AggregatingMergeTree() ORDER BY cid;

INSERT INTO test_uniq_theta_intersect_91595 VALUES (1, arrayReduce('uniqThetaState', [1, 2]::Array(UInt32))), (2, arrayReduce('uniqThetaState', [2, 3]::Array(UInt32)));

SELECT finalizeAggregation(
    uniqThetaIntersect(
        uniqThetaMergeStateIf(users, cid = 1),
        uniqThetaMergeStateIf(users, cid = 3) -- cid = 3 never matches -> empty state
    )
)
FROM test_uniq_theta_intersect_91595;

DROP TABLE test_uniq_theta_intersect_91595;
