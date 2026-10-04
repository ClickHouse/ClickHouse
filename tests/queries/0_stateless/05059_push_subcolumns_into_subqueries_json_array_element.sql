-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

-- `FunctionToSubcolumnsPass` rewrites the chained `Dynamic`/JSON-array access `json.a[1].b` of a
-- column read from a table into `arrayElement` over the nested subcolumn ``json.a.:`Array(JSON)`.b``.
-- That pass only handles table sources, so the same expression over a column exported by a subquery
-- is rewritten here instead, otherwise the whole `json.a` value would be materialized.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_json_array;
DROP TABLE IF EXISTS t_push_subcolumns_json_array_final;

CREATE TABLE t_push_subcolumns_json_array
(
    id UInt32,
    json JSON,
    d Dynamic
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_json_array VALUES (1, '{"a": [{"b": 1, "c": {"e": 10}}, {"b": 2, "c": {"e": 20}}], "z": 100}', [1, 2, 3]);
INSERT INTO t_push_subcolumns_json_array VALUES (2, '{"a": [{"b": 3, "c": {"e": 30}}], "z": 200}', [4]);

SELECT 'one field';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x.a[1].b FROM (SELECT json AS x FROM t_push_subcolumns_json_array)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT toString(x.a[1].b) AS v FROM (SELECT json AS x FROM t_push_subcolumns_json_array) ORDER BY v;

SELECT 'nested fields';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x.a[1].c.e FROM (SELECT json AS x FROM t_push_subcolumns_json_array)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT toString(x.a[1].c.e) AS v FROM (SELECT json AS x FROM t_push_subcolumns_json_array) ORDER BY v;

SELECT 'two levels of subqueries';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT y.a[1].b FROM (SELECT x AS y FROM (SELECT json AS x FROM t_push_subcolumns_json_array))
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT toString(y.a[1].b) AS v FROM (SELECT x AS y FROM (SELECT json AS x FROM t_push_subcolumns_json_array)) ORDER BY v;

SELECT 'through a CTE';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 WITH cte AS (SELECT json AS x FROM t_push_subcolumns_json_array) SELECT x.a[1].b FROM cte
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
WITH cte AS (SELECT json AS x FROM t_push_subcolumns_json_array) SELECT toString(x.a[1].b) AS v FROM cte ORDER BY v;

SELECT 'UNION ALL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x.a[1].b FROM
    (
        SELECT json AS x FROM t_push_subcolumns_json_array
        UNION ALL
        SELECT json AS x FROM t_push_subcolumns_json_array
    )
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT toString(x.a[1].b) AS v FROM
(
    SELECT json AS x FROM t_push_subcolumns_json_array
    UNION ALL
    SELECT json AS x FROM t_push_subcolumns_json_array
) ORDER BY v;

SELECT 'a Dynamic column that is not a JSON path is not rewritten (as in the direct read)';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x[1].b FROM (SELECT d AS x FROM t_push_subcolumns_json_array)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';

SELECT 'FINAL is not rewritten into a direct read of the nested subcolumn';
CREATE TABLE t_push_subcolumns_json_array_final
(
    id UInt32,
    json JSON
)
ENGINE = ReplacingMergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_json_array_final VALUES (1, '{"a": [{"b": 1}, {"b": 2}]}');
INSERT INTO t_push_subcolumns_json_array_final VALUES (1, '{"a": [{"b": 9}]}'), (2, '{"a": [{"b": 3}]}');

SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x.a[1].b FROM (SELECT json AS x FROM t_push_subcolumns_json_array_final FINAL)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT toString(x.a[1].b) AS v FROM (SELECT json AS x FROM t_push_subcolumns_json_array_final FINAL) ORDER BY v;

SELECT 'the whole column stays alive';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x.a[1].b, x FROM (SELECT json AS x FROM t_push_subcolumns_json_array)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';

SELECT 'setting off';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x.a[1].b FROM (SELECT json AS x FROM t_push_subcolumns_json_array)
    SETTINGS optimize_push_subcolumns_into_subqueries = 0
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';

DROP TABLE t_push_subcolumns_json_array;
DROP TABLE t_push_subcolumns_json_array_final;
