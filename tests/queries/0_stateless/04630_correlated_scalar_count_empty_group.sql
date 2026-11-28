-- A correlated scalar subquery whose projection is built from counting aggregates (count(), uniqExact(), ...)
-- and constants must return its value over an empty group (count() -> 0), not NULL, when the correlated group
-- is empty for an outer row. Other aggregates (sum, min, max, avg) still return NULL for an empty correlated
-- group. See #111615.

SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;

-- count(): empty correlated group (k=1) must be 0, not NULL.
SELECT o.k, (SELECT count() FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- uniqExact(): same, empty group -> 0.
SELECT o.k, (SELECT uniqExact(i.flag) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- countDistinct() and countIfDistinct(): empty group -> 0 also with the uniqCombined/uniqCombined64 implementations.
SELECT o.k, (SELECT countDistinct(i.flag) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k
SETTINGS count_distinct_implementation = 'uniqCombined', aggregate_functions_null_for_empty = 0;

SELECT o.k, (SELECT countIfDistinct(i.flag, i.flag = 1) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k
SETTINGS count_distinct_implementation = 'uniqCombined64', aggregate_functions_null_for_empty = 0;

-- sum() empty group stays NULL, so `WHERE outer > (SELECT sum ...)` keeps filtering empty groups
-- (deliberate, see QueryNode::getResultType).
SELECT o.k, (SELECT sum(i.flag) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS s
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- -OrDefault asks for the default over an empty input, so its empty group is 0.
SELECT o.k,
    (SELECT sumOrDefault(i.flag) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS s,
    (SELECT avgOrDefault(i.flag) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS a
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k
SETTINGS aggregate_functions_null_for_empty = 0;

-- min()/max() empty group stays NULL.
SELECT o.k, (SELECT min(i.flag) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS mn
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- No-match vs a legitimate NULL over a non-empty group must both stay correct: k=1 has one row with v=NULL
-- so max(v)=NULL (legitimate), k=9 has no group (no match) -> also NULL. count() over the same must be 1 / 0.
SELECT o.k,
    (SELECT max(i.v) FROM values('k UInt8, v Nullable(Int32)', (1, NULL), (2, 5)) AS i WHERE i.k = o.k) AS mx,
    (SELECT count() FROM values('k UInt8, v Nullable(Int32)', (1, NULL), (2, 5)) AS i WHERE i.k = o.k) AS cnt
FROM values('k UInt8', (1), (2), (9)) AS o
ORDER BY o.k;

-- An expression over counting aggregates gets its value over the empty group (count() + 100 -> 100). With any
-- other aggregate it stays NULL. Without an aggregate there is no row for the empty group, so it stays NULL too.
SELECT o.k,
    (SELECT count() + 100 FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS c,
    (SELECT toString(count() * 2 + uniqExact(i.flag)) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS s,
    (SELECT count() + sum(i.flag) FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS m,
    (SELECT 100 FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS n
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- The expression is evaluated like a one-row query: a short-circuited branch that would throw is skipped, and a
-- non-constant result such as materialize(count()) is restored too.
SELECT o.k,
    (SELECT if(count() = 0, 100, intDiv(100, count())) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c_if,
    (SELECT multiIf(count() = 0, 100, intDiv(100, count())) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c_multi_if,
    (SELECT materialize(count()) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c_materialize
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k;

-- A LowCardinality projection is restored too. A LowCardinality(Nullable) one can be NULL over a non-empty
-- group (k=2), so it keeps the NULL.
SELECT o.k,
    (SELECT toLowCardinality(toString(count())) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c_lc,
    (SELECT toLowCardinality(nullIf(toString(count()), '1')) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c_lc_nullable
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k;

-- The restored column is still a LowCardinality column, not only typed as one.
SELECT o.k,
    dumpColumnStructure((SELECT toLowCardinality(toString(count())) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k))
        LIKE 'LowCardinality(Nullable(String)), LowCardinality(%'
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k;

-- A non-deterministic or side-effecting function cannot be evaluated once for every empty group, so the restore
-- is not applied.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT o.k,
    (SELECT count() + rand() FROM values('k UInt8', (1), (2)) AS i WHERE i.k = o.k),
    (SELECT count() + sleep(0) FROM values('k UInt8', (1), (2)) AS i WHERE i.k = o.k)
    FROM values('k UInt8', (1), (2)) AS o)
WHERE explain ILIKE '%multiIf(%rand()%' OR explain ILIKE '%multiIf(%sleep(%';

-- An expression that would throw over an empty group must not fail a query without empty groups.
SELECT o.k, (SELECT intDiv(100, count()) FROM values('k UInt8', (1), (2)) AS i WHERE i.k = o.k) AS d
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k;

-- A memory limit either keeps the empty-group value (1 MB fits) or fails the query (1 GB does not), never gives NULL.
SELECT o.k, (SELECT length(repeat(repeat('a', 1000), if(count() = 0, 1000, 1))) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k
SETTINGS max_memory_usage = 100000000;

SELECT o.k, (SELECT length(repeat(repeat('a', 1000), if(count() = 0, 1000000, 1))) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k
SETTINGS max_memory_usage = 100000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- A 1 MB empty-group value is copied only into the one empty group's row, not into the 1000 matched ones.
SELECT sum(length(c))
FROM (SELECT (SELECT repeat('x', if(count() = 0, 1000000, 1)) FROM numbers(1000) AS i WHERE i.number = o.number) AS c FROM numbers(1001) AS o)
SETTINGS max_memory_usage = 100000000, max_threads = 1, max_block_size = 1000;

SELECT sum(length(c))
FROM (SELECT (SELECT repeat('x', if(count() = 0, 1000000, 1)) FROM numbers(1000) AS i WHERE i.number = o.number) AS c FROM numbers(1001) AS o)
SETTINGS max_memory_usage = 100000000, max_threads = 1, max_block_size = 1000, correlated_subqueries_default_join_kind = 'left';

-- count(<Nullable column>) over a MergeTree table: optimize_functions_to_subcolumns rewrites it to
-- countIf(not(col.null)) in a correlated subquery (sum(not(col.null)) elsewhere), so the empty group is 0 with
-- either setting and only the null map is read.
DROP TABLE IF EXISTS t04630_inner;
DROP TABLE IF EXISTS t04630_outer;
CREATE TABLE t04630_inner (k UInt8, v Nullable(Int32)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t04630_inner VALUES (1, NULL), (2, 5);
CREATE TABLE t04630_outer (k UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t04630_outer VALUES (1), (2), (9);

SELECT o.k, (SELECT count(i.v) FROM t04630_inner AS i WHERE i.k = o.k) AS c
FROM t04630_outer AS o
ORDER BY o.k
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT o.k, (SELECT count(i.v) FROM t04630_inner AS i WHERE i.k = o.k) AS c
FROM t04630_outer AS o
ORDER BY o.k
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT countIf(explain LIKE '%function_name: countIf,%'), countIf(explain LIKE '%column_name: v.null,%'), countIf(explain LIKE '%column_name: v,%')
FROM (EXPLAIN QUERY TREE SELECT o.k, (SELECT count(i.v) FROM t04630_inner AS i WHERE i.k = o.k) FROM t04630_outer AS o
    SETTINGS optimize_functions_to_subcolumns = 1);

DROP TABLE t04630_inner;
DROP TABLE t04630_outer;

-- empty_result_for_aggregation_by_empty_set = 1 makes an empty aggregation produce no row, so even count()
-- over an empty group is NULL; the restore must be disabled in that mode.
SELECT o.k, (SELECT count() FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k
SETTINGS empty_result_for_aggregation_by_empty_set = 1;

-- The same setting on the SUBQUERY's own SETTINGS context must also disable the restore: the empty group
-- stays NULL even though the outer query leaves the setting at 0 (the gate reads the subquery node's context).
SELECT o.k, (SELECT count() FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1 SETTINGS empty_result_for_aggregation_by_empty_set = 1) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- Matches on every outer row (no empty group) is unaffected.
SELECT o.k, (SELECT count() FROM values('k UInt8', (1), (2), (2)) AS i WHERE i.k = o.k) AS c
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k;

-- The restore also works with the LEFT decorrelation join kind.
SELECT o.k, (SELECT count() FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k
SETTINGS correlated_subqueries_default_join_kind = 'left';

-- An inner GROUP BY makes an empty result legitimately absent, so the empty group stays NULL (guard).
SELECT o.k, (SELECT count() FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1 GROUP BY i.flag) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- An inner HAVING can drop every group, so the result stays NULL (guard).
SELECT o.k, (SELECT count() FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1 HAVING count() > 10) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- An inner LIMIT 0 or OFFSET 1 removes the aggregate's only row, so the result stays NULL for every outer row (guards).
SELECT o.k, (SELECT count() FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1 LIMIT 0) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

SELECT o.k, (SELECT count() FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS i WHERE i.k = o.k AND i.flag = 1 OFFSET 1) AS c
FROM values('k UInt8, flag UInt8', (1, 0), (2, 1)) AS o
ORDER BY o.k;

-- Plain `=` with a nullable key: NULL = NULL is unknown, so the NULL outer row has no match and count() = 0
-- (SQL-standard) while sum() = NULL.
SELECT o.k,
    (SELECT count() FROM values('k Nullable(Int32)', (NULL), (NULL), (5)) AS i WHERE i.k = o.k) AS c,
    (SELECT sum(i.k) FROM values('k Nullable(Int32)', (NULL), (NULL), (5)) AS i WHERE i.k = o.k) AS s
FROM values('k Nullable(Int32)', (NULL), (5)) AS o
ORDER BY o.k NULLS LAST;

-- Null-safe match (IS NOT DISTINCT FROM) over a nullable key: the decorrelation join matches a NULL outer
-- value to its own group null-safely, so the NULL outer row counts the two inner NULL rows.
SELECT o.k, (SELECT count() FROM values('k Nullable(Int32)', (NULL), (NULL), (5)) AS i WHERE i.k IS NOT DISTINCT FROM o.k) AS c
FROM values('k Nullable(Int32)', (NULL), (5)) AS o
ORDER BY o.k NULLS LAST;

-- [count()] cannot be made Nullable, so the restore is not applied. Assert the restore's multiIf wrapper is
-- present for count() but absent for [count()], which pins the Nullable-result guard (a plain result oracle
-- cannot: an unmatched Array is already join-filled with []).
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT o.k, (SELECT count() FROM values('k UInt8', (1), (2)) AS i WHERE i.k = o.k) FROM values('k UInt8', (1), (2)) AS o)
WHERE explain ILIKE '%multiIf(%count()%';

SELECT count()
FROM (EXPLAIN actions = 1 SELECT o.k, (SELECT [count()] FROM values('k UInt8', (1), (2)) AS i WHERE i.k = o.k) FROM values('k UInt8', (1), (2)) AS o)
WHERE explain ILIKE '%multiIf(%array(count())%';

-- countOrNull() inherits count()'s returns_default_when_only_null but already returns a Nullable result, so it
-- can legitimately be NULL over a non-empty group; the restore must not apply (no multiIf wrapper in the plan).
SELECT count()
FROM (EXPLAIN actions = 1 SELECT o.k, (SELECT countOrNull() FROM values('k UInt8', (1), (2)) AS i WHERE i.k = o.k) FROM values('k UInt8', (1), (2)) AS o)
WHERE explain ILIKE '%multiIf(%countOrNull%';

-- A composite (Tuple) result inherits returns_default_when_only_null (quantilesExactTuple) but its empty value
-- ([0],[0]) is not the type default ([],[]), so the restore must not apply (only plain-number results are). The
-- empty group keeps the natural join-fill; assert no multiIf wrapper is added for it.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT o.k, (SELECT quantilesExactTuple(0.5)(i.t) FROM (SELECT 2 AS k, (5, 6)::Tuple(UInt64, UInt64) AS t) AS i WHERE i.k = o.k) FROM values('k UInt8', (1), (2)) AS o)
WHERE explain ILIKE '%multiIf(%quantilesExactTuple%';

-- Lambdas and constant IN sets in the projection are built by the planner's own actions visitor, so their
-- empty-group value is restored too, with both decorrelation join kinds.
SELECT o.k,
    (SELECT arraySum(arrayMap(x -> x + 100, [count()])) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS a,
    (SELECT arraySum(arrayMap(x -> x + count(), [100])) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS b,
    (SELECT count() IN (0, 1) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c,
    (SELECT count() NOT IN (0, 5) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS d
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k;

SELECT o.k,
    (SELECT arraySum(arrayMap(x -> x + 100, [count()])) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS a,
    (SELECT count() IN (0, 1) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k
SETTINGS correlated_subqueries_default_join_kind = 'left';

-- arrayJoin after the aggregation can drop the row of a non-empty group (b, d at k=2), which the join also
-- reports as NULL, so a projection or ORDER BY with arrayJoin is never restored, with both decorrelation join kinds.
SELECT o.k,
    (SELECT arrayJoin(arrayFilter(x -> x != 0, [count()])) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS a,
    (SELECT arrayJoin(arrayFilter(x -> x = 0, [count()])) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS b,
    (SELECT arrayJoin([count() + 100]) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c,
    (SELECT count() FROM values('k UInt8', (2)) AS i WHERE i.k = o.k ORDER BY arrayJoin(arrayFilter(x -> x = 0, [count()]))) AS d
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k;

SELECT o.k,
    (SELECT arrayJoin(arrayFilter(x -> x != 0, [count()])) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS a,
    (SELECT arrayJoin(arrayFilter(x -> x = 0, [count()])) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS b,
    (SELECT arrayJoin([count() + 100]) FROM values('k UInt8', (2)) AS i WHERE i.k = o.k) AS c,
    (SELECT count() FROM values('k UInt8', (2)) AS i WHERE i.k = o.k ORDER BY arrayJoin(arrayFilter(x -> x = 0, [count()]))) AS d
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k
SETTINGS correlated_subqueries_default_join_kind = 'left';

-- Same-named columns of different types (a, b: UInt8 and UInt64; d, e: Bool and UInt8) have one empty-group input,
-- so the empty group keeps NULL in either operand order. Of the same type (c) that input is the value of both, so it
-- is restored.
SELECT o.k,
    (SELECT bitCount(bitNot(minOrDefault(i.v))) + bitCount(bitNot(minOrDefault(j.v)))
     FROM values('k UInt8, v UInt8', (2, 7)) AS i CROSS JOIN values('k UInt8, v UInt64', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS a,
    (SELECT bitCount(bitNot(minOrDefault(j.v))) + bitCount(bitNot(minOrDefault(i.v)))
     FROM values('k UInt8, v UInt8', (2, 7)) AS i CROSS JOIN values('k UInt8, v UInt64', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS b,
    (SELECT bitCount(bitNot(minOrDefault(i.v))) + bitCount(bitNot(minOrDefault(j.v)))
     FROM values('k UInt8, v UInt8', (2, 7)) AS i CROSS JOIN values('k UInt8, v UInt8', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS c,
    (SELECT toString(minOrDefault(i.v)) || ',' || toString(minOrDefault(j.v))
     FROM values('k UInt8, v Bool', (2, true)) AS i CROSS JOIN values('k UInt8, v UInt8', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS d,
    (SELECT toString(minOrDefault(j.v)) || ',' || toString(minOrDefault(i.v))
     FROM values('k UInt8, v Bool', (2, true)) AS i CROSS JOIN values('k UInt8, v UInt8', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS e
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k
SETTINGS aggregate_functions_null_for_empty = 0;

SELECT o.k,
    (SELECT bitCount(bitNot(minOrDefault(i.v))) + bitCount(bitNot(minOrDefault(j.v)))
     FROM values('k UInt8, v UInt8', (2, 7)) AS i CROSS JOIN values('k UInt8, v UInt64', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS a,
    (SELECT bitCount(bitNot(minOrDefault(j.v))) + bitCount(bitNot(minOrDefault(i.v)))
     FROM values('k UInt8, v UInt8', (2, 7)) AS i CROSS JOIN values('k UInt8, v UInt64', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS b,
    (SELECT bitCount(bitNot(minOrDefault(i.v))) + bitCount(bitNot(minOrDefault(j.v)))
     FROM values('k UInt8, v UInt8', (2, 7)) AS i CROSS JOIN values('k UInt8, v UInt8', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS c,
    (SELECT toString(minOrDefault(i.v)) || ',' || toString(minOrDefault(j.v))
     FROM values('k UInt8, v Bool', (2, true)) AS i CROSS JOIN values('k UInt8, v UInt8', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS d,
    (SELECT toString(minOrDefault(j.v)) || ',' || toString(minOrDefault(i.v))
     FROM values('k UInt8, v Bool', (2, true)) AS i CROSS JOIN values('k UInt8, v UInt8', (2, 5)) AS j
     WHERE i.k = o.k AND j.k = o.k) AS e
FROM values('k UInt8', (1), (2)) AS o
ORDER BY o.k
SETTINGS correlated_subqueries_default_join_kind = 'left', aggregate_functions_null_for_empty = 0;
