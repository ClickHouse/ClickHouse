-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

-- `json['a']` over a column exported by a subquery reads the combined subcolumn `json.@`a``
-- instead of the whole JSON column, like `FunctionToSubcolumnsPass` does for direct table reads.
-- In a chain `json['c']['d']` the inner access is pushed, so only `json.@`c`` is read.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_json_bracket;

CREATE TABLE t_push_subcolumns_json_bracket
(
    id UInt32,
    json JSON(t UInt32)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_json_bracket VALUES
    (1, '{"a" : 1, "c" : {"d" : "x"}, "t" : 10}'),
    (2, '{"a" : "s", "c" : {"d" : 2, "e" : 3}}'),
    (3, '{"b" : 1}');

SELECT 'single key';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x['a'] FROM (SELECT json AS x FROM t_push_subcolumns_json_bracket)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT x['a'], toTypeName(x['a']) FROM (SELECT id, json AS x FROM t_push_subcolumns_json_bracket) ORDER BY id;
SELECT json['a'], toTypeName(json['a']) FROM t_push_subcolumns_json_bracket ORDER BY id;

SELECT 'typed path';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x['t'] FROM (SELECT json AS x FROM t_push_subcolumns_json_bracket)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT x['t'], toTypeName(x['t']) FROM (SELECT id, json AS x FROM t_push_subcolumns_json_bracket) ORDER BY id;
SELECT json['t'], toTypeName(json['t']) FROM t_push_subcolumns_json_bracket ORDER BY id;

SELECT 'chain through a CTE';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 WITH cte AS (SELECT id, json AS x FROM t_push_subcolumns_json_bracket) SELECT x['c']['d'] FROM cte
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
WITH cte AS (SELECT id, json AS x FROM t_push_subcolumns_json_bracket) SELECT x['c']['d'] FROM cte ORDER BY id;
SELECT json['c']['d'] FROM t_push_subcolumns_json_bracket ORDER BY id;

SELECT 'two levels of subqueries';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT y['c'] FROM (SELECT x AS y FROM (SELECT json AS x FROM t_push_subcolumns_json_bracket))
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT y['c'] FROM (SELECT id, x AS y FROM (SELECT id, json AS x FROM t_push_subcolumns_json_bracket)) ORDER BY id;

SELECT 'a key with a dot is not rewritten';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x['c.d'] FROM (SELECT json AS x FROM t_push_subcolumns_json_bracket)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';

SELECT 'a subcolumn of the exported value is not composed with the JSON path';
SELECT y.Int64, y.String FROM (SELECT id, x['a'] AS y FROM (SELECT id, json AS x FROM t_push_subcolumns_json_bracket)) ORDER BY id;

DROP TABLE t_push_subcolumns_json_bracket;
