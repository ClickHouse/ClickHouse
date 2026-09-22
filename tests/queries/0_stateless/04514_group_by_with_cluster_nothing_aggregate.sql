-- Regression: an aggregate with a zero-size state (`AggregateFunctionNothing`, produced by
-- `min(NULL, ...)` / `max(NULL)` / `any(NULL)` and similar) cannot be given a distinct per-row
-- state: a zero-byte arena allocation does not advance the arena, so every row's state pointer
-- aliases the same address (and `ensureAggregateStateOwnership` cannot de-alias a zero-size state
-- either). `ClusterMergingTransform` folds group states in place via `IAggregateFunction::merge`,
-- which asserts the source and destination states do not alias, so clustering a query that carries
-- such a stateless aggregate raised a `LOGICAL_ERROR`. The merge is a no-op for a stateless
-- aggregate and must be skipped. (AST fuzzer STID 2508-320f.)
-- See https://github.com/ClickHouse/ClickHouse/pull/101878

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;

-- 1D: `ts` = 1 and 5 fall in one cluster (distance 10); merging them must not choke on the
-- stateless `min(NULL, ts)` column. Five clusters remain: {1,5}, {100}, {200}, {255}, {2147483648}.
SELECT '1d';
SELECT count() AS num_clusters, sum(total) AS grand_total
FROM (
    SELECT min(NULL, ts) AS nothing_state, sum(value) AS total
    FROM VALUES('ts UInt64, value UInt64', (1, 10), (5, 20), (255, 1048576), (100, 40), (2147483648, 7), (200, 60))
    GROUP BY ts WITH CLUSTER 10
);

-- 2D: same, with a `(x, y)` cluster key; (0,0) and (0.5,0.5) merge (Euclidean distance <= 1).
SELECT '2d';
SELECT count() AS num_clusters, sum(total) AS grand_total
FROM (
    SELECT min(NULL, x) AS nothing_state, sum(v) AS total
    FROM VALUES('x Float64, y Float64, v UInt64', (0, 0, 1), (0.5, 0.5, 2), (100, 100, 4))
    GROUP BY (x, y) WITH CLUSTER 1
);

-- String: Levenshtein-clustered keys ('cat' / 'bat' merge at distance 1) with a stateless aggregate.
SELECT 'string';
SELECT count() AS num_clusters, sum(total) AS grand_total
FROM (
    SELECT min(NULL, s) AS nothing_state, sum(v) AS total
    FROM VALUES('s String, v UInt64', ('cat', 1), ('bat', 2), ('zzz', 4))
    GROUP BY s WITH CLUSTER 1
);
