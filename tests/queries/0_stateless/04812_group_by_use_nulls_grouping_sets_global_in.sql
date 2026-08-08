-- Tags: shard

-- Regression test: `group_by_use_nulls` promotes a GROUPING SETS key's declared type to
-- Nullable(UInt8), but `FunctionIn` produced a plain UInt8 column, aborting the server with
-- `Unexpected return type from globalIn. Expected Nullable(UInt8). Got UInt8`.
--
-- A table identifier on the right of GLOBAL IN is load-bearing: the set is then a FutureSet
-- filled during distributed execution, so header computation runs the function against an
-- unready set. With a subquery the set is built beforehand and the mismatch never surfaces.

-- The promotion under test is the analyzer's: the old analyzer's `appendGroupByModifiers`
-- returns early for GROUPING SETS, so no key is promoted there and `UInt8` is correct.
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

-- LowCardinality left argument: the framework strips LowCardinality before calling the
-- function, so this checks that such an argument does not change the reconciliation.
-- `serialize_query_plan = 0` keeps this arm off the unrelated issue #112028, where a
-- LowCardinality argument to `IN` loses that wrapper across plan serialization.
SELECT 'low cardinality left';
SELECT toTypeName(materialize(toLowCardinality(1)) GLOBAL IN (t_04812)) AS t,
       materialize(toLowCardinality(1)) GLOBAL IN (t_04812) AS s
FROM remote('127.0.0.1', system, one)
GROUP BY GROUPING SETS ((materialize(toLowCardinality(1)) GLOBAL IN (t_04812)))
SETTINGS group_by_use_nulls = 1, serialize_query_plan = 0;

-- Control: with a literal set the declared type is LowCardinality(Nullable(UInt8)). The
-- reconciliation must leave that wrapper alone, so the type must stay LowCardinality here.
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

-- Control: without group_by_use_nulls the declared type is not Nullable, so the reconciliation
-- must be inert and the result must stay plain UInt8.
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

DROP TABLE t_04812;
DROP TABLE t_04812_empty;
