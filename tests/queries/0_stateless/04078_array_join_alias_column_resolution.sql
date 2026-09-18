-- https://github.com/ClickHouse/ClickHouse/issues/83434
-- ARRAY JOIN over a Map column must resolve ALIAS subcolumns that share the
-- column's name prefix (`a.k = mapKeys(a)`, `a.v = mapValues(a)`) element-wise,
-- the same way the old analyzer did. Previously the new analyzer failed with
-- "Unknown expression identifier `aa.k`".

DROP TABLE IF EXISTS t_array_join_alias;

CREATE TABLE t_array_join_alias
(
    a Map(String, UInt64),
    `a.k` Array(String) ALIAS mapKeys(a),
    `a.v` Array(UInt64) ALIAS mapValues(a)
) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO t_array_join_alias VALUES (map('x', 10, 'y', 20)), (map('z', 30));

SELECT 'aliased array join';
SELECT aa.k, aa.v FROM t_array_join_alias ARRAY JOIN a AS aa ORDER BY aa.k;

SELECT 'unaliased array join';
SELECT a.k, a.v FROM t_array_join_alias ARRAY JOIN a ORDER BY a.k;

DROP TABLE t_array_join_alias;
