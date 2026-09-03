-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

-- A map with Enum keys can also be indexed by the name of the enum value: `m['a']` arrives as a
-- String constant that does not insert into the Enum key column directly and is converted through
-- the enum name first, the same way `FunctionToSubcolumnsPass` handles direct table reads.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_enum_map;

CREATE TABLE t_push_subcolumns_enum_map
(
    id UInt32,
    m Map(Enum8('a' = 1, 'b' = 2), UInt32)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_enum_map VALUES (1, map('a', 10)), (2, map('b', 20, 'a', 30)), (3, map('b', 40));

SELECT 'indexing by the enum name';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x['a'] FROM (SELECT m AS x FROM t_push_subcolumns_enum_map)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT x['a'] FROM (SELECT m AS x FROM t_push_subcolumns_enum_map) ORDER BY 1;

SELECT 'indexing by the numeric value of the enum is not rewritten (as in the direct read)';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x[2] FROM (SELECT m AS x FROM t_push_subcolumns_enum_map)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT x[2] FROM (SELECT m AS x FROM t_push_subcolumns_enum_map) ORDER BY 1;

SELECT 'through a CTE';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 WITH cte AS (SELECT m AS x FROM t_push_subcolumns_enum_map) SELECT x['b'] FROM cte
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
WITH cte AS (SELECT m AS x FROM t_push_subcolumns_enum_map) SELECT x['b'] FROM cte ORDER BY 1;

SELECT 'setting off';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x['a'] FROM (SELECT m AS x FROM t_push_subcolumns_enum_map)
    SETTINGS optimize_push_subcolumns_into_subqueries = 0
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';

DROP TABLE t_push_subcolumns_enum_map;
