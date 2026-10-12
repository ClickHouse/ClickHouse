-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

-- `tupleElement` over a `Nullable(Tuple(...))` column exported by a subquery reads the element
-- subcolumn when the element can contain NULL, like `FunctionToSubcolumnsPass` does for direct
-- table reads. An element that cannot contain NULL gets a default value for an outer NULL from
-- `tupleElement`, which differs from the subcolumn, so it keeps reading the whole column.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_nullable_tuple;

CREATE TABLE t_push_subcolumns_nullable_tuple
(
    id UInt32,
    n Nullable(Tuple(a Nullable(UInt32), b Array(UInt32)))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_nullable_tuple VALUES (1, (1, [1])), (2, NULL), (3, (NULL, [2, 3]));

SELECT 'nullable element';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT tupleElement(x, 'a') FROM (SELECT n AS x FROM t_push_subcolumns_nullable_tuple)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT tupleElement(x, 'a') FROM (SELECT id, n AS x FROM t_push_subcolumns_nullable_tuple) ORDER BY id;
SELECT tupleElement(n, 'a') FROM t_push_subcolumns_nullable_tuple ORDER BY id;

SELECT 'nullable element through a CTE';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 WITH cte AS (SELECT n AS x FROM t_push_subcolumns_nullable_tuple) SELECT x.a FROM cte
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
WITH cte AS (SELECT id, n AS x FROM t_push_subcolumns_nullable_tuple) SELECT x.a FROM cte ORDER BY id;

SELECT 'element that cannot contain NULL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT tupleElement(x, 'b') FROM (SELECT n AS x FROM t_push_subcolumns_nullable_tuple)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT tupleElement(x, 'b') FROM (SELECT id, n AS x FROM t_push_subcolumns_nullable_tuple) ORDER BY id;
SELECT tupleElement(n, 'b') FROM t_push_subcolumns_nullable_tuple ORDER BY id;

DROP TABLE t_push_subcolumns_nullable_tuple;
