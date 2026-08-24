DROP TABLE IF EXISTS t_in_empty_set;
DROP TABLE IF EXISTS t_in_empty_set_nullable;
DROP TABLE IF EXISTS t_in_empty_set_lc;

CREATE TABLE t_in_empty_set (a Int, b Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_in_empty_set VALUES (1, 2);

SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN ();
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a NOT IN ();
SELECT x FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a NOT IN ();
SELECT count() FROM t_in_empty_set LEFT ARRAY JOIN [b] AS x WHERE a IN ();
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN _CAST([], 'Array(Int32)');
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE toString(a) IN ();
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE multiIf(a IN (), 1, 0);
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a GLOBAL NOT IN ();

SELECT count() FROM t_in_empty_set WHERE a IN ();
SELECT a IN (), a NOT IN () FROM t_in_empty_set;

SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN (SELECT number FROM numbers(3));
SELECT count() FROM t_in_empty_set ARRAY JOIN [b] AS x WHERE a IN (SELECT a FROM t_in_empty_set WHERE 0);

CREATE TABLE t_in_empty_set_nullable (a Nullable(Int), b Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_in_empty_set_nullable VALUES (1, 2), (NULL, 3);

SELECT count() FROM t_in_empty_set_nullable ARRAY JOIN [b] AS x WHERE a IN ();
SELECT count() FROM t_in_empty_set_nullable ARRAY JOIN [b] AS x WHERE nullIn(a, ());
SELECT a, a IN (), a NOT IN () FROM t_in_empty_set_nullable ORDER BY a NULLS LAST;
SELECT a, nullIn(a, ()), notNullIn(a, ()) FROM t_in_empty_set_nullable ORDER BY a NULLS LAST;

CREATE TABLE t_in_empty_set_lc (a LowCardinality(String), b Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_in_empty_set_lc VALUES ('x', 2);

SELECT count() FROM t_in_empty_set_lc ARRAY JOIN [b] AS x WHERE a IN ();
SELECT count() FROM t_in_empty_set_lc ARRAY JOIN [b] AS x WHERE a NOT IN ();
SELECT toTypeName(a IN ()) FROM t_in_empty_set_lc;

DROP TABLE t_in_empty_set;
DROP TABLE t_in_empty_set_nullable;
DROP TABLE t_in_empty_set_lc;
