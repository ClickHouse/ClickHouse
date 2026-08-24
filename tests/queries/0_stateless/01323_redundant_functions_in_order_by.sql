SET single_join_prefer_left_table = 0;

DROP TABLE IF EXISTS test;

CREATE TABLE test (key UInt64, a UInt8, b String, c Float64) ENGINE = MergeTree() ORDER BY key;
INSERT INTO test SELECT number, number, toString(number), number from numbers(4);

set optimize_redundant_functions_in_order_by = 1;

SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x)) SETTINGS enable_analyzer=1;
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x)));
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x))) SETTINGS enable_analyzer=1;
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x) SETTINGS enable_analyzer=1;
SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN test t USING(key) ORDER BY s.key, t.key;
SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN test t USING(key) ORDER BY s.key, t.key SETTINGS enable_analyzer=1;
SELECT key, a FROM test ORDER BY key, a, exp(key + a);
SELECT key, a FROM test ORDER BY key, a, exp(key + a) SETTINGS enable_analyzer=1;
SELECT key, a FROM test ORDER BY key, exp(key + a);
SELECT key, a FROM test ORDER BY key, exp(key + a) SETTINGS enable_analyzer=1;
EXPLAIN SYNTAX SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
EXPLAIN QUERY TREE run_passes=1 SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x)) settings enable_analyzer=1;
EXPLAIN SYNTAX SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x)));
EXPLAIN QUERY TREE run_passes=1 SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x))) settings enable_analyzer=1;
EXPLAIN SYNTAX SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
EXPLAIN QUERY TREE run_passes=1 SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x) settings enable_analyzer=1;
EXPLAIN SYNTAX SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN test t USING(key) ORDER BY s.key, t.key;
EXPLAIN QUERY TREE run_passes=1 SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN test t USING(key) ORDER BY s.key, t.key settings enable_analyzer=1;
EXPLAIN SYNTAX SELECT key, a FROM test ORDER BY key, a, exp(key + a);
EXPLAIN QUERY TREE run_passes=1 SELECT key, a FROM test ORDER BY key, a, exp(key + a) settings enable_analyzer=1;
EXPLAIN SYNTAX SELECT key, a FROM test ORDER BY key, exp(key + a);
EXPLAIN QUERY TREE run_passes=1 SELECT key, a FROM test ORDER BY key, exp(key + a) settings enable_analyzer=1;
EXPLAIN QUERY TREE run_passes=1 SELECT key FROM test GROUP BY key ORDER BY avg(a), key settings enable_analyzer=1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (id UInt64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t2 (id UInt64) ENGINE = MergeTree() ORDER BY id;

EXPLAIN QUERY TREE run_passes=1 SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY t1.id, t2.id settings enable_analyzer=1;

set optimize_redundant_functions_in_order_by = 0;

SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x)));
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN test t USING(key) ORDER BY s.key, t.key;
SELECT key, a FROM test ORDER BY key, a, exp(key + a);
SELECT key, a FROM test ORDER BY key, exp(key + a);
EXPLAIN SYNTAX SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
EXPLAIN SYNTAX SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x)));
EXPLAIN SYNTAX SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
EXPLAIN SYNTAX SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN test t USING(key) ORDER BY s.key, t.key;
EXPLAIN SYNTAX SELECT key, a FROM test ORDER BY key, a, exp(key + a);
EXPLAIN SYNTAX SELECT key, a FROM test ORDER BY key, exp(key + a);

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE test;
