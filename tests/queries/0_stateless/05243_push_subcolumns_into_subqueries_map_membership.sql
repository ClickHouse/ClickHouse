-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

-- Map membership carriers (`has`, `notHas`, `mapContainsValue`, `mapContainsKeyLike`, `mapContainsValueLike`)
-- are pushed into subqueries as reads of `keys` / `values`, mirroring `FunctionToSubcolumnsPass`,
-- and `isNull` is rewritten into a comparison of the null map with zero.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_map_membership;

CREATE TABLE t_push_subcolumns_map_membership
(
    id UInt32,
    m Map(String, String),
    lm Map(LowCardinality(String), String),
    n Nullable(UInt32)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_map_membership VALUES (1, {'a': '1', 'bc': '2'}, {'a': '1'}, NULL), (2, {}, {}, 5), (3, {'x': '7'}, {'x': '7'}, 0);

SELECT 'pushed';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT has(m, 'a'), notHas(m, 'a'), mapContainsValue(m, '7'), mapContainsKeyLike(m, 'b%'), mapContainsValueLike(m, '2%'), isNull(n)
    FROM (SELECT m, n FROM t_push_subcolumns_map_membership)
)
WHERE explain LIKE '%Output%';

SELECT id, has(m, 'a'), notHas(m, 'a'), mapContainsValue(m, '7'), mapContainsKeyLike(m, 'b%'), mapContainsValueLike(m, '2%'), isNull(n)
FROM (SELECT id, m, n FROM t_push_subcolumns_map_membership) ORDER BY id;

SELECT 'setting off';
SELECT id, has(m, 'a'), notHas(m, 'a'), mapContainsValue(m, '7'), mapContainsKeyLike(m, 'b%'), mapContainsValueLike(m, '2%'), isNull(n)
FROM (SELECT id, m, n FROM t_push_subcolumns_map_membership) ORDER BY id
SETTINGS optimize_push_subcolumns_into_subqueries = 0;

-- `has` / `notHas` over a Map with LowCardinality keys are not rewritten, like in `FunctionToSubcolumnsPass`.
SELECT 'LowCardinality key';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT has(lm, 'a'), notHas(lm, 'x') FROM (SELECT lm FROM t_push_subcolumns_map_membership)
)
WHERE explain LIKE '%Output%';
SELECT id, has(lm, 'a'), notHas(lm, 'x') FROM (SELECT id, lm FROM t_push_subcolumns_map_membership) ORDER BY id;

-- A non-constant LIKE pattern is not moved into a lambda.
SELECT 'non-constant pattern';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT mapContainsKeyLike(m, p) FROM (SELECT m, 'b%' AS p FROM t_push_subcolumns_map_membership)
)
WHERE explain LIKE '%Output%';

-- A Nullable needle is not rewritten.
SELECT 'Nullable needle';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT has(m, toNullable('a')) FROM (SELECT m FROM t_push_subcolumns_map_membership)
)
WHERE explain LIKE '%Output%';

DROP TABLE t_push_subcolumns_map_membership;
