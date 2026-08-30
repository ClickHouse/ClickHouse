-- Two date-time values that hold the same number but carry different time zones are different
-- expressions: a date or time function reads the time zone from its argument, so it returns a
-- different result for each of them. No optimization may substitute one for the other.
-- Epoch 0 is hour 0 in UTC and hour 9 in Asia/Tokyo.

SET enable_analyzer = 1;
-- CI may inject 1: reading in parallel replicas refuses a projection and reshapes the plans asserted below.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS tz_identity;
CREATE TABLE tz_identity (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tz_identity VALUES (0);

-- Both predicates hold, so their conjunction holds. All three values are printed so that the row
-- is self-checking.
SELECT
    'and of two true terms',
    toHour(toDateTime(0, 'UTC') + x) = 0,
    toHour(toDateTime(0, 'Asia/Tokyo') + x) = 9,
    (toHour(toDateTime(0, 'UTC') + x) = 0) AND (toHour(toDateTime(0, 'Asia/Tokyo') + x) = 9)
FROM (SELECT materialize(0)::UInt32 AS x)
SETTINGS optimize_redundant_comparisons = 1;

-- The second term is false, so the conjunction is false.
SELECT 'and of a true and a false term',
    (toHour(toDateTime(0, 'UTC') + x) = 0) AND (toHour(toDateTime(0, 'Asia/Tokyo') + x) = 0)
FROM (SELECT materialize(0)::UInt32 AS x)
SETTINGS optimize_redundant_comparisons = 1;

SELECT 'and of two ranges',
    (toHour(toDateTime(0, 'UTC') + x) < 5) AND (toHour(toDateTime(0, 'Asia/Tokyo') + x) > 5)
FROM (SELECT materialize(0)::UInt32 AS x)
SETTINGS optimize_redundant_comparisons = 1;

SELECT 'and of an equality and an inequality',
    (toHour(toDateTime(0, 'UTC') + x) = 0) AND (toHour(toDateTime(0, 'Asia/Tokyo') + x) != 0)
FROM (SELECT materialize(0)::UInt32 AS x)
SETTINGS optimize_redundant_comparisons = 1;

-- A chain of inequalities long enough to be rewritten into a single NOT IN. The middle term is
-- false, so the chain is false.
SELECT 'chain of inequalities',
    (toHour(toDateTime(0, 'UTC') + x) != 5)
    AND (toHour(toDateTime(0, 'Asia/Tokyo') + x) != 9)
    AND (toHour(toDateTime(0, 'UTC') + x) != 7)
FROM (SELECT materialize(0)::UInt32 AS x)
SETTINGS optimize_min_inequality_conjunction_chain_length = 3;

-- Three terms in one zone are one chain and become one NOT IN. The same three across two zones are
-- two shorter chains, so neither reaches the length that is rewritten.
SELECT 'chain of inequalities in one zone is rewritten', count() FROM (
    EXPLAIN QUERY TREE
    SELECT (toHour(toDateTime(0, 'UTC') + x) != 5)
        AND (toHour(toDateTime(0, 'UTC') + x) != 9)
        AND (toHour(toDateTime(0, 'UTC') + x) != 7)
    FROM (SELECT materialize(0)::UInt32 AS x)
    SETTINGS optimize_min_inequality_conjunction_chain_length = 3
) WHERE explain ILIKE '%notIn%';

SELECT 'chain of inequalities across zones is not rewritten', count() FROM (
    EXPLAIN QUERY TREE
    SELECT (toHour(toDateTime(0, 'UTC') + x) != 5)
        AND (toHour(toDateTime(0, 'Asia/Tokyo') + x) != 9)
        AND (toHour(toDateTime(0, 'UTC') + x) != 7)
    FROM (SELECT materialize(0)::UInt32 AS x)
    SETTINGS optimize_min_inequality_conjunction_chain_length = 3
) WHERE explain ILIKE '%notIn%';

SELECT 'and of two true terms, sub-second precision',
    (toHour(toDateTime64(0, 3, 'UTC') + x) = 0) AND (toHour(toDateTime64(0, 3, 'Asia/Tokyo') + x) = 9)
FROM (SELECT materialize(0)::UInt32 AS x)
SETTINGS optimize_redundant_comparisons = 1;

-- The time zone is part of the type, so it still tells the two apart when the type is nested.
SELECT 'time zone inside a tuple',
    (toHour(tuple(toDateTime(0, 'UTC'), 1).1 + x) = 0)
    AND (toHour(tuple(toDateTime(0, 'Asia/Tokyo'), 1).1 + x) = 9)
FROM (SELECT materialize(0)::UInt32 AS x)
SETTINGS optimize_redundant_comparisons = 1;

-- The reported form: both predicates hold for the stored row, so the row must survive the WHERE.
-- No settings are pinned, because the answer does not depend on which optimization runs.
SELECT 'and of two true terms over a table', count() FROM tz_identity
WHERE toHour(toDateTime(0, 'UTC') + x) = 0 AND toHour(toDateTime(0, 'Asia/Tokyo') + x) = 9;

-- One filter over another, which the query plan merges into a single expression.
SELECT 'filter over filter',
    count()
FROM (SELECT x FROM tz_identity WHERE toHour(toDateTime(0, 'UTC') + x) = 0)
WHERE toHour(toDateTime(0, 'Asia/Tokyo') + x) = 9
SETTINGS query_plan_merge_expressions = 1, query_plan_merge_filters = 1;

-- The merged filter needs one `plus` per zone, so the pair below counts them. The same-zone pair
-- must collapse to one: that is what shows the count follows the merge instead of being a constant.
SELECT 'filter over filter keeps both terms', count() FROM (
    EXPLAIN actions = 1, compact = 0
    SELECT count()
    FROM (SELECT x FROM tz_identity WHERE toHour(toDateTime(0, 'UTC') + x) = 0)
    WHERE toHour(toDateTime(0, 'Asia/Tokyo') + x) = 9
    SETTINGS query_plan_merge_expressions = 1, query_plan_merge_filters = 1
) WHERE explain ILIKE '%FUNCTION plus(%';

SELECT 'filter over filter merges one term', count() FROM (
    EXPLAIN actions = 1, compact = 0
    SELECT count()
    FROM (SELECT x FROM tz_identity WHERE toHour(toDateTime(0, 'UTC') + x) = 0)
    WHERE toHour(toDateTime(0, 'UTC') + x) = 0
    SETTINGS query_plan_merge_expressions = 1, query_plan_merge_filters = 1
) WHERE explain ILIKE '%FUNCTION plus(%';

-- Two different sort keys must both survive. Ordering by hour in the two zones differs only by a
-- constant rotation, so the rows come out in the same order either way and only the plan can tell.
SELECT 'two distinct sort keys', count() FROM (
    EXPLAIN QUERY TREE
    SELECT x FROM (SELECT materialize(0)::UInt32 AS x)
    ORDER BY toHour(toDateTime(0, 'UTC') + x), toHour(toDateTime(0, 'Asia/Tokyo') + x)
) WHERE explain ILIKE '%SORT id%';

-- The same holds with no enclosing function: the two constants are themselves the two sort keys.
SELECT 'two distinct constant sort keys', count() FROM (
    EXPLAIN QUERY TREE
    SELECT x FROM (SELECT materialize(0)::UInt32 AS x)
    ORDER BY toDateTime(0, 'UTC'), toDateTime(0, 'Asia/Tokyo')
) WHERE explain ILIKE '%SORT id%';

-- The rest must behave exactly as before: an optimization that should fire still fires, and no
-- analysis that compares a node with itself starts to fail.
SELECT 'contradiction in one zone', (toHour(toDateTime(0, 'UTC') + x) = 0) AND (toHour(toDateTime(0, 'UTC') + x) = 9)
FROM (SELECT materialize(0)::UInt32 AS x)
SETTINGS optimize_redundant_comparisons = 1;

SELECT 'contradiction in one zone is folded away', count() FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tz_identity
    WHERE toHour(toDateTime(0, 'UTC') + x) = 0 AND toHour(toDateTime(0, 'UTC') + x) = 9
) WHERE explain ILIKE '%Filter column: 0%';

SELECT 'repeated sort key', count() FROM (
    EXPLAIN QUERY TREE
    SELECT x FROM (SELECT materialize(0)::UInt32 AS x)
    ORDER BY toHour(toDateTime(0, 'UTC') + x), toHour(toDateTime(0, 'UTC') + x)
) WHERE explain ILIKE '%SORT id%';

SELECT 'group by', toHour(toDateTime(0, 'UTC') + x)
FROM (SELECT materialize(0)::UInt32 AS x)
GROUP BY toHour(toDateTime(0, 'UTC') + x);

SELECT 'group by then order by', toHour(toDateTime(0, 'UTC') + x)
FROM (SELECT materialize(0)::UInt32 AS x)
GROUP BY toHour(toDateTime(0, 'UTC') + x)
ORDER BY toHour(toDateTime(0, 'UTC') + x);

SELECT 'array join', toHour(a)
FROM (SELECT [toDateTime(0, 'UTC'), toDateTime(0, 'UTC')] AS arr)
ARRAY JOIN arr AS a;

DROP TABLE tz_identity;

-- An aggregate projection is matched against the query by comparing the two expression trees, which
-- is a third place the zone has to survive. A separate table keeps the projection out of the arms
-- above.
DROP TABLE IF EXISTS tz_projection;
CREATE TABLE tz_projection (x UInt32,
    PROJECTION pr_utc_hour (SELECT toHour(toDateTime(0, 'UTC') + x), count()
                            GROUP BY toHour(toDateTime(0, 'UTC') + x)))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tz_projection VALUES (0);

SELECT 'aggregate projection in another zone',
    toHour(toDateTime(0, 'Asia/Tokyo') + x), count()
FROM tz_projection
GROUP BY toHour(toDateTime(0, 'Asia/Tokyo') + x)
SETTINGS optimize_use_projections = 1;

SELECT 'aggregate projection is not read in another zone', count() FROM (
    EXPLAIN
    SELECT toHour(toDateTime(0, 'Asia/Tokyo') + x), count()
    FROM tz_projection
    GROUP BY toHour(toDateTime(0, 'Asia/Tokyo') + x)
    SETTINGS optimize_use_projections = 1
) WHERE explain ILIKE '%ReadFromMergeTree (pr_utc_hour)%';

SELECT 'aggregate projection is still read in its own zone', count() FROM (
    EXPLAIN
    SELECT toHour(toDateTime(0, 'UTC') + x), count()
    FROM tz_projection
    GROUP BY toHour(toDateTime(0, 'UTC') + x)
    SETTINGS optimize_use_projections = 1
) WHERE explain ILIKE '%ReadFromMergeTree (pr_utc_hour)%';

-- A projection keyed on one zone answers a query that omits the zone only while the session resolves
-- to that same zone. The stored hour is the hour of the projection's zone, not of the session's.
SELECT 'omitted zone against a projection on another zone', toHour(toDateTime(0) + x), count()
FROM tz_projection GROUP BY toHour(toDateTime(0) + x)
SETTINGS optimize_use_projections = 1, session_timezone = 'Asia/Tokyo';

SELECT 'omitted zone against a projection on that same zone', toHour(toDateTime(0) + x), count()
FROM tz_projection GROUP BY toHour(toDateTime(0) + x)
SETTINGS optimize_use_projections = 1, session_timezone = 'UTC';

DROP TABLE tz_projection;

-- A time zone left out of the type means the session time zone, so leaving it out and spelling it out
-- are one expression. Alias, `GROUP BY` and `GROUPING` analysis report a difference here as an error
-- rather than as a slower plan, so these are the arms where telling the two apart would reject valid
-- queries. `session_timezone` is randomized in CI, so each arm pins the zone it needs.
SELECT 'implicit and explicit zone under one alias', toDateTime(0) AS a, toDateTime(0, 'UTC') AS a
SETTINGS session_timezone = 'UTC';

SELECT 'implicit and explicit zone under one alias, non-UTC session', toDateTime(0) AS a, toDateTime(0, 'Asia/Tokyo') AS a
SETTINGS session_timezone = 'Asia/Tokyo';

-- One expression is also one hash key, so the two spellings collapse into a single sort key.
SELECT 'an omitted zone and that zone spelled out are one sort key', count() FROM (
    EXPLAIN QUERY TREE
    SELECT x FROM (SELECT materialize(0)::UInt32 AS x)
    ORDER BY toDateTime(0), toDateTime(0, 'UTC')
) WHERE explain ILIKE '%SORT id%'
SETTINGS session_timezone = 'UTC';

-- Two zones under one alias are two expressions, so the alias is ambiguous. Before this change the
-- query returned a different value in each column while claiming both were the same expression.
SELECT 'two zones under one alias', toDateTime(0, 'UTC') AS a, toDateTime(0, 'Asia/Tokyo') AS a
SETTINGS session_timezone = 'UTC'; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- A group key spelled differently from the selected expression passes name analysis and then has no
-- column to read, which is how this shape behaves without this change too.
SELECT 'implicit and explicit zone as a group key', toHour(toDateTime(0) + x)
FROM (SELECT materialize(0)::UInt32 AS x)
GROUP BY toHour(toDateTime(0, 'UTC') + x)
SETTINGS session_timezone = 'UTC'; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

SELECT 'implicit and explicit zone as a grouping argument', grouping(toHour(toDateTime(0) + x))
FROM (SELECT materialize(0)::UInt32 AS x)
GROUP BY toHour(toDateTime(0, 'UTC') + x)
WITH ROLLUP
SETTINGS session_timezone = 'UTC'; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

-- The hour in another zone is a different expression, so it is not one of the group keys.
SELECT 'another zone as a group key', toHour(toDateTime(0, 'Asia/Tokyo') + x)
FROM (SELECT materialize(0)::UInt32 AS x)
GROUP BY toHour(toDateTime(0, 'UTC') + x)
SETTINGS session_timezone = 'UTC'; -- { serverError NOT_AN_AGGREGATE }

-- An omitted zone is the only difference excused: a declared name of its own is a different
-- expression, both where the two are compared and where they are used as hash keys.
SELECT 'a custom name under one alias',
    toDateTime(0, 'UTC')::SimpleAggregateFunction(any, DateTime('UTC')) AS a, toDateTime(0, 'UTC') AS a
SETTINGS session_timezone = 'UTC'; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- The everyday member of that class: `Bool` is `UInt8` with a name of its own, so the two spellings
-- of the number one are two expressions.
SELECT 'a declared name under one alias', true AS a, toUInt8(1) AS a; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'a custom name is its own sort key', count() FROM (
    EXPLAIN QUERY TREE
    SELECT x FROM (SELECT materialize(0)::UInt32 AS x)
    ORDER BY toDateTime(0, 'UTC')::SimpleAggregateFunction(any, DateTime('UTC')), toDateTime(0, 'UTC')
) WHERE explain ILIKE '%SORT id%'
SETTINGS session_timezone = 'UTC';

SELECT 'a declared name is its own sort key', count() FROM (
    EXPLAIN QUERY TREE
    SELECT x FROM (SELECT materialize(0)::UInt32 AS x)
    ORDER BY true, toUInt8(1)
) WHERE explain ILIKE '%SORT id%';
