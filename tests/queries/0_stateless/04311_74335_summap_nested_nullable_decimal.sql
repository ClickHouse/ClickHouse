-- https://github.com/ClickHouse/ClickHouse/issues/74335
-- `sumMap` over a `Nested(... Nullable(Decimal(P, S)))` column used to throw
-- "Bad get: has Decimal32, requested Decimal128" whenever the aggregate
-- state was serialised: for example, when parallel replicas marshal
-- partial states between coordinator and replicas, or when `sumMapState`
-- is materialised via a binary-state formatter.

DROP TABLE IF EXISTS sum_map_nested_nullable_decimal;

CREATE TABLE sum_map_nested_nullable_decimal
(
    `statusMap` Nested(goal_id UInt16, revenue Nullable(Decimal(9, 5)))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO sum_map_nested_nullable_decimal VALUES
    ([1, 2, 3], [1.0, 2.0, 3.0]),
    ([3, 4, 5], [3.0, 4.0, 5.0]),
    ([4, 5, 6], [4.0, 5.0, 6.0]),
    ([6, 7, 8], [6.0, 7.0, 8.0]);

SELECT 'in-memory aggregate (always worked)';
SELECT sumMap(statusMap.goal_id, statusMap.revenue) FROM sum_map_nested_nullable_decimal;

SELECT 'serialised state (used to throw BAD_GET)';
SELECT length(toString(sumMapState(statusMap.goal_id, statusMap.revenue))) > 0
FROM sum_map_nested_nullable_decimal;

SELECT 'state round-trip via sumMapState + sumMapMerge';
SELECT sumMapMerge(s) FROM (
    SELECT sumMapState(statusMap.goal_id, statusMap.revenue) AS s
    FROM sum_map_nested_nullable_decimal
);

SELECT 'parallel replicas (the original reproducer)';
SELECT sumMap(statusMap.goal_id, statusMap.revenue)
FROM sum_map_nested_nullable_decimal
SETTINGS
    automatic_parallel_replicas_mode = 0,
    enable_parallel_replicas = 2,
    max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Same checks for `Decimal64` to cover the second promoted branch.
DROP TABLE sum_map_nested_nullable_decimal;
CREATE TABLE sum_map_nested_nullable_decimal
(
    `statusMap` Nested(goal_id UInt16, revenue Nullable(Decimal(18, 5)))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO sum_map_nested_nullable_decimal VALUES
    ([1, 2, 3], [1.0, 2.0, 3.0]),
    ([3, 4, 5], [3.0, 4.0, 5.0]);

SELECT 'Nullable(Decimal64) serialised state';
SELECT length(toString(sumMapState(statusMap.goal_id, statusMap.revenue))) > 0
FROM sum_map_nested_nullable_decimal;

SELECT 'Nullable(Decimal64) state round-trip';
SELECT sumMapMerge(s) FROM (
    SELECT sumMapState(statusMap.goal_id, statusMap.revenue) AS s
    FROM sum_map_nested_nullable_decimal
);

-- Exercise the NULL path so we cover the value.isNull() branch in `serialize`.
DROP TABLE sum_map_nested_nullable_decimal;
CREATE TABLE sum_map_nested_nullable_decimal
(
    `statusMap` Nested(goal_id UInt16, revenue Nullable(Decimal(9, 5)))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO sum_map_nested_nullable_decimal VALUES
    ([1, 2],    [NULL, 2.0]),
    ([2, 3],    [3.0, NULL]),
    ([1, 3],    [NULL, NULL]);

SELECT 'NULL values mixed with non-NULL (state round-trip)';
SELECT sumMapMerge(s) FROM (
    SELECT sumMapState(statusMap.goal_id, statusMap.revenue) AS s
    FROM sum_map_nested_nullable_decimal
);

DROP TABLE sum_map_nested_nullable_decimal;
