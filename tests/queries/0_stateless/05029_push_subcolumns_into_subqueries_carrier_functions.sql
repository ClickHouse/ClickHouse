-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

-- Functions that are not the subcolumn itself but an expression over it: `FunctionToSubcolumnsPass`
-- rewrites them only when the column is read directly from a table, so they have to be pushed
-- into the subquery by this pass, otherwise the whole parent column is read.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_carriers;

CREATE TABLE t_push_subcolumns_carriers
(
    id UInt32,
    arr Array(UInt32),
    s String,
    m Map(String, UInt32),
    n Nullable(UInt32),
    tup Tuple(a String, b UInt32)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_carriers VALUES
    (1, [1, 2], 'one', {'a': 10, 'b': 20}, 1, ('a', 1)),
    (2, [], '', {'c': 30}, NULL, ('x', 2));

SELECT 'empty of an Array';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT empty(a) FROM (SELECT arr AS a FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT empty(a) FROM (SELECT arr AS a FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'notEmpty of an Array';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT notEmpty(a) FROM (SELECT arr AS a FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT notEmpty(a) FROM (SELECT arr AS a FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'empty and length share the size0 subcolumn';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT empty(a), length(a) FROM (SELECT arr AS a FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT empty(a), length(a) FROM (SELECT arr AS a FROM t_push_subcolumns_carriers) ORDER BY 1, 2;

SELECT 'empty of a String';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT empty(x) FROM (SELECT s AS x FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT empty(x) FROM (SELECT s AS x FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'empty of a Map';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT empty(x) FROM (SELECT m AS x FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT empty(x) FROM (SELECT m AS x FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'mapContainsKey';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT mapContainsKey(x, 'a') FROM (SELECT m AS x FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT mapContainsKey(x, 'a') FROM (SELECT m AS x FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'isNotNull';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT isNotNull(x) FROM (SELECT n AS x FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT isNotNull(x) FROM (SELECT n AS x FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'isNull and isNotNull share the null subcolumn';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT isNull(x), isNotNull(x) FROM (SELECT n AS x FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT isNull(x), isNotNull(x) FROM (SELECT n AS x FROM t_push_subcolumns_carriers) ORDER BY 1, 2;

SELECT 'count of a Nullable';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT count(x) FROM (SELECT n AS x FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT count(x) FROM (SELECT n AS x FROM t_push_subcolumns_carriers);

SELECT 'count of a Nullable with GROUP BY';
SELECT count(x) FROM (SELECT n AS x, s AS y FROM t_push_subcolumns_carriers) GROUP BY y ORDER BY 1;

SELECT 'through a CTE and two levels of subqueries';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1
    WITH c AS (SELECT a AS b FROM (SELECT arr AS a FROM t_push_subcolumns_carriers))
    SELECT empty(b) FROM c
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
WITH c AS (SELECT a AS b FROM (SELECT arr AS a FROM t_push_subcolumns_carriers))
SELECT empty(b) FROM c ORDER BY 1;

SELECT 'through UNION ALL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1
    SELECT isNotNull(x) FROM (SELECT n AS x FROM t_push_subcolumns_carriers UNION ALL SELECT n AS x FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT isNotNull(x) FROM (SELECT n AS x FROM t_push_subcolumns_carriers UNION ALL SELECT n AS x FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'the whole column is read anyway';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT empty(a), a FROM (SELECT arr AS a FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT empty(a), a FROM (SELECT arr AS a FROM t_push_subcolumns_carriers) ORDER BY 1, 2;

SELECT 'a defaultable JOIN side is not rewritten';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1
    SELECT isNotNull(r.x) FROM t_push_subcolumns_carriers AS l
    LEFT JOIN (SELECT n AS x, id FROM t_push_subcolumns_carriers) AS r ON l.id = r.id + 100
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT isNotNull(r.x) FROM t_push_subcolumns_carriers AS l
LEFT JOIN (SELECT n AS x, id FROM t_push_subcolumns_carriers) AS r ON l.id = r.id + 100 ORDER BY 1;

SELECT 'the map key is itself a pushed subcolumn';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1
    SELECT mapContainsKey(x, y.a) FROM (SELECT m AS x, tup AS y FROM t_push_subcolumns_carriers)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Output:%';
SELECT mapContainsKey(x, y.a) FROM (SELECT m AS x, tup AS y FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'a non-constant map key is not rewritten into has';
SELECT mapContainsKey(x, y) FROM (SELECT m AS x, s AS y FROM t_push_subcolumns_carriers) ORDER BY 1;

SELECT 'the setting is off';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT empty(a) FROM (SELECT arr AS a FROM t_push_subcolumns_carriers)
    SETTINGS optimize_push_subcolumns_into_subqueries = 0
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';

DROP TABLE t_push_subcolumns_carriers;

SELECT 'QBit element read through a subquery';

SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS t_push_subcolumns_qbit;

CREATE TABLE t_push_subcolumns_qbit (id UInt32, v QBit(BFloat16, 8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_push_subcolumns_qbit VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]);

SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT tupleElement(x, 2) FROM (SELECT v AS x FROM t_push_subcolumns_qbit)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT hex(tupleElement(x, 2)) FROM (SELECT v AS x FROM t_push_subcolumns_qbit);

DROP TABLE t_push_subcolumns_qbit;
