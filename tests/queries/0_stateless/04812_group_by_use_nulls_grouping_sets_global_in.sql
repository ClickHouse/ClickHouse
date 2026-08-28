-- Tags: shard

-- `GLOBAL IN` built a separate temporary external table for every syntactic occurrence of its
-- right-hand side, because `buildQueryTreeForShard` keyed its dedup map on an alias-sensitive tree
-- hash while `createUniqueAliasesIfNecessary` had already given each occurrence its own `__tableN`.
-- Repeated occurrences of one expression therefore got different `__set_` action node names, so a
-- GROUP BY key repeated in the SELECT list was not recognised as the same expression: the projection
-- rebuilt it instead of reading the aggregated column, and the rebuilt node carried the
-- `group_by_use_nulls` promotion its function did not produce, aborting the server with
-- `Unexpected return type from globalIn. Expected Nullable(UInt8). Got UInt8`.
--
-- A table identifier on the right of GLOBAL IN is load-bearing: the set is then a FutureSet filled
-- during distributed execution, so header computation runs the function against an unready set.
-- With a subquery the set is built beforehand and the mismatch never surfaces.

-- The promotion under test is the analyzer's: the old analyzer's `appendGroupByModifiers` returns
-- early for GROUPING SETS, so no key is promoted there and `UInt8` is correct.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_04812;
DROP TABLE IF EXISTS t_04812_empty;

CREATE TABLE t_04812 (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04812 VALUES (1);

CREATE TABLE t_04812_empty (x UInt8) ENGINE = MergeTree ORDER BY x;

-- Each arm selects the raw expression as well as its type: a `toTypeName`-only projection is
-- constant-folded before the failing check and would pass even on the unfixed server.

SELECT 'globalIn';
SELECT toTypeName(1 GLOBAL IN (t_04812)) AS t, 1 GLOBAL IN (t_04812) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((1 GLOBAL IN (t_04812)))
SETTINGS group_by_use_nulls = 1;

SELECT 'globalNotIn';
SELECT toTypeName(1 GLOBAL NOT IN (t_04812)) AS t, 1 GLOBAL NOT IN (t_04812) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((1 GLOBAL NOT IN (t_04812)))
SETTINGS group_by_use_nulls = 1;

SELECT 'globalNullIn';
SELECT toTypeName(globalNullIn(1, t_04812)) AS t, globalNullIn(1, t_04812) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((globalNullIn(1, t_04812)))
SETTINGS group_by_use_nulls = 1;

SELECT 'globalNotNullIn';
SELECT toTypeName(globalNotNullIn(1, t_04812)) AS t, globalNotNullIn(1, t_04812) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((globalNotNullIn(1, t_04812)))
SETTINGS group_by_use_nulls = 1;

-- Empty set: a distinct query shape that still runs the function against an unready set while
-- the header is computed, so it witnesses the same defect independently of the arms above.
SELECT 'empty set';
SELECT toTypeName(1 GLOBAL IN (t_04812_empty)) AS t, 1 GLOBAL IN (t_04812_empty) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((1 GLOBAL IN (t_04812_empty)))
SETTINGS group_by_use_nulls = 1;

-- LowCardinality left argument. The declared type is LowCardinality(UInt8), which
-- `makeNullableOrLowCardinalityNullableSafe` promotes to LowCardinality(Nullable(UInt8)); the
-- wrapper must survive. `serialize_query_plan = 0` keeps this arm off the unrelated issue #112028,
-- where a LowCardinality argument to `IN` loses that wrapper across plan serialization.
SELECT 'low cardinality left';
SELECT toTypeName(materialize(toLowCardinality(1)) GLOBAL IN (t_04812)) AS t,
       materialize(toLowCardinality(1)) GLOBAL IN (t_04812) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((materialize(toLowCardinality(1)) GLOBAL IN (t_04812)))
SETTINGS group_by_use_nulls = 1, serialize_query_plan = 0;

-- Control: a literal set reaches the same LowCardinality(Nullable(UInt8)) declared type.
SELECT 'control low cardinality result type';
SELECT toTypeName(materialize(toLowCardinality('a')) GLOBAL IN ('a', 'b')) AS t,
       materialize(toLowCardinality('a')) GLOBAL IN ('a', 'b') AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((materialize(toLowCardinality('a')) GLOBAL IN ('a', 'b')))
SETTINGS group_by_use_nulls = 1;

-- Control: local IN keeps its Nullable(UInt8) type and its value.
SELECT 'control local in';
SELECT toTypeName(1 IN (t_04812)) AS t, 1 IN (t_04812) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((1 IN (t_04812)))
SETTINGS group_by_use_nulls = 1;

-- Control: without group_by_use_nulls no key is promoted, so the type stays plain UInt8.
SELECT 'control no group_by_use_nulls';
SELECT toTypeName(1 GLOBAL IN (t_04812)) AS t, 1 GLOBAL IN (t_04812) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((1 GLOBAL IN (t_04812)));

-- Control: a subquery set does not reach the failing exit, and its type is unchanged.
SELECT 'control subquery set';
SELECT toTypeName(1 GLOBAL IN (SELECT 1)) AS t, 1 GLOBAL IN (SELECT 1) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((1 GLOBAL IN (SELECT 1)))
SETTINGS group_by_use_nulls = 1;

-- The arms above all use a single grouping set, where the key is always present and a rebuilt node
-- happens to compute the same value. The arms below vary the shape so that rebuilding is
-- observable. Each pairs a local `IN` control with the `GLOBAL IN` witness: the two must agree,
-- since `GLOBAL` changes only where the set is built, never the result.

-- The super-aggregate row of the empty grouping set must report the key as NULL. Only the column
-- the aggregator widened carries that NULL; re-evaluating the expression yields the value instead.
SELECT 'super-aggregate row, IN';
SELECT 1 IN (t_04812) AS s, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY GROUPING SETS ((1 IN (t_04812)), ())
ORDER BY s NULLS LAST
SETTINGS group_by_use_nulls = 1;

SELECT 'super-aggregate row, GLOBAL IN';
SELECT 1 GLOBAL IN (t_04812) AS s, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY GROUPING SETS ((1 GLOBAL IN (t_04812)), ())
ORDER BY s NULLS LAST
SETTINGS group_by_use_nulls = 1;

-- A non-constant left operand cannot be re-evaluated after aggregation at all, because the input
-- column is gone by then. This arm fails loudly where the constant one fails silently.
SELECT 'non-constant left, IN';
SELECT number IN (t_04812) AS s, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY GROUPING SETS ((number IN (t_04812)), ())
ORDER BY s NULLS LAST
SETTINGS group_by_use_nulls = 1;

SELECT 'non-constant left, GLOBAL IN';
SELECT number GLOBAL IN (t_04812) AS s, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY GROUPING SETS ((number GLOBAL IN (t_04812)), ())
ORDER BY s NULLS LAST
SETTINGS group_by_use_nulls = 1;

-- `GROUPING` resolves its argument against the GROUP BY keys by action node name, so it reports the
-- key as absent whenever the two occurrences are named differently. That makes it the direct probe
-- for the shared set: matching names are only possible when one temporary table serves both.
SELECT 'GROUPING(), IN';
SELECT 1 IN (t_04812) AS s, GROUPING(1 IN (t_04812)) AS g, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY GROUPING SETS ((1 IN (t_04812)), ())
ORDER BY g
SETTINGS group_by_use_nulls = 1;

SELECT 'GROUPING(), GLOBAL IN';
SELECT 1 GLOBAL IN (t_04812) AS s, GROUPING(1 GLOBAL IN (t_04812)) AS g, count()
FROM remote('127.0.0.1', numbers(2))
GROUP BY GROUPING SETS ((1 GLOBAL IN (t_04812)), ())
ORDER BY g
SETTINGS group_by_use_nulls = 1;

DROP TABLE t_04812;
DROP TABLE t_04812_empty;
