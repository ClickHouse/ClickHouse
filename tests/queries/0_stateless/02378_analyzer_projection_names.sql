SET enable_analyzer = 1;
SET single_join_prefer_left_table = 0;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

DROP TABLE IF EXISTS test_table_in;
CREATE TABLE test_table_in
(
    id UInt64
) ENGINE=TinyLog;

DROP TABLE IF EXISTS test_table_compound;
CREATE TABLE test_table_compound
(
    id UInt64,
    tuple_value Tuple(value_1 UInt64, value_2 String)
) ENGINE=TinyLog;

INSERT INTO test_table_compound VALUES (0, tuple(0, 'Value'));

DROP TABLE IF EXISTS test_table_join_1;
CREATE TABLE test_table_join_1
(
    id UInt64,
    value String,
    value_join_1 String
) ENGINE=TinyLog;

INSERT INTO test_table_join_1 VALUES (0, 'Join_1_Value', 'Join_1_Value');

DROP TABLE IF EXISTS test_table_join_2;
CREATE TABLE test_table_join_2
(
    id UInt64,
    value String,
    value_join_2 String
) ENGINE=TinyLog;

INSERT INTO test_table_join_2 VALUES (0, 'Join_2_Value', 'Join_2_Value');

DROP TABLE IF EXISTS test_table_join_3;
CREATE TABLE test_table_join_3
(
    id UInt64,
    value String,
    value_join_3 String
) ENGINE=TinyLog;

INSERT INTO test_table_join_3 VALUES (0, 'Join_3_Value', 'Join_3_Value');

-- { echoOn }

SELECT 'Constants';

DESCRIBE (SELECT 1, 'Value');

SELECT '--';

DESCRIBE (SELECT 1 + 1, concat('Value_1', 'Value_2'));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)'));

SELECT 'Columns';

DESCRIBE (SELECT test_table.id, test_table.id, id FROM test_table);

SELECT '--';

DESCRIBE (SELECT * FROM test_table);

SELECT '--';

DESCRIBE (SELECT * APPLY toString FROM test_table);

SELECT '--';

DESCRIBE (SELECT * APPLY x -> toString(x) FROM test_table);

SELECT '--';

DESCRIBE (SELECT tuple_value.* FROM test_table_compound);

SELECT '--';

DESCRIBE (SELECT tuple_value.* APPLY x -> x FROM test_table_compound);

SELECT '--';

DESCRIBE (SELECT tuple_value.* APPLY toString FROM test_table_compound);

SELECT '--';

DESCRIBE (SELECT tuple_value.* APPLY x -> toString(x) FROM test_table_compound);

SELECT 'Constants with aliases';

DESCRIBE (SELECT 1 AS a, a AS b, b, b AS c, c, 'Value' AS d, d AS e, e AS f);

SELECT '--';

DESCRIBE (SELECT plus(1 AS a, a AS b), plus(b, b), plus(b, b) AS c, concat('Value' AS d, d) AS e, e);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS a, a.id, a.value);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS a, a.*);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS a, a.* EXCEPT id);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS a, a.* EXCEPT value);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS a, a.* EXCEPT value APPLY toString);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS a, a.* EXCEPT value APPLY x -> toString(x));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS a, untuple(a));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS a, untuple(a) AS b);

SELECT 'Columns with aliases';

DESCRIBE (SELECT test_table.id AS a, a, test_table.id AS b, b AS c, c FROM test_table);

SELECT '--';

DESCRIBE (SELECT plus(test_table.id AS a, test_table.id), plus(id, id AS b), plus(b, b), plus(test_table.id, test_table.id) FROM test_table);

SELECT '--';

DESCRIBE (SELECT test_table.* REPLACE id + (id AS id_alias) AS id, id_alias FROM test_table);

SELECT 'Matcher';

DESCRIBE (SELECT * FROM test_table);

SELECT '--';

DESCRIBE (SELECT test_table.* FROM test_table);

SELECT '--';

DESCRIBE (SELECT 1 AS id, 2 AS value, * FROM test_table);

SELECT '--';

DESCRIBE (SELECT 1 AS id, 2 AS value, * FROM test_table AS t1);

SELECT 'Lambda';

DESCRIBE (SELECT arrayMap(x -> x + 1, [1,2,3]));

SELECT '--';

DESCRIBE (SELECT 1 AS a, arrayMap(x -> x + a, [1,2,3]));

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> x + test_table.id + test_table.id + id, [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> x + (test_table.id AS first) + (test_table.id AS second) + id, [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> test_table.* EXCEPT value, [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> test_table.* EXCEPT value APPLY x -> x, [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> test_table.* EXCEPT value APPLY toString, [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> test_table.* EXCEPT value APPLY x -> toString(x), [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1), 'Tuple (id UInt64)') AS compound_value, arrayMap(x -> compound_value.*, [1,2,3]));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1), 'Tuple (id UInt64)') AS compound_value, arrayMap(x -> compound_value.* APPLY x -> x, [1,2,3]));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1), 'Tuple (id UInt64)') AS compound_value, arrayMap(x -> compound_value.* APPLY toString, [1,2,3]));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1), 'Tuple (id UInt64)') AS compound_value, arrayMap(x -> compound_value.* APPLY x -> toString(x), [1,2,3]));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS compound_value, arrayMap(x -> compound_value.* EXCEPT value, [1,2,3]));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS compound_value, arrayMap(x -> compound_value.* EXCEPT value APPLY x -> x, [1,2,3]));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS compound_value, arrayMap(x -> compound_value.* EXCEPT value APPLY toString, [1,2,3]));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1, 'Value'), 'Tuple (id UInt64, value String)') AS compound_value, arrayMap(x -> compound_value.* EXCEPT value APPLY x -> toString(x), [1,2,3]));

SELECT '--';

DESCRIBE (SELECT cast(tuple(1), 'Tuple (id UInt64)') AS a, arrayMap(x -> untuple(a), [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1), 'Tuple (id UInt64)') AS a, arrayMap(x -> untuple(a) AS untupled_value, [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1), 'Tuple (id UInt64)') AS a, untuple(a) AS untupled_value, arrayMap(x -> untupled_value, [1,2,3]) FROM test_table);

SELECT '--';

DESCRIBE (SELECT cast(tuple(1), 'Tuple (id UInt64)') AS a, untuple(a) AS untupled_value, arrayMap(x -> untupled_value AS untupled_value_in_lambda, [1,2,3]) FROM test_table);

SELECT 'Standalone lambda';

DESCRIBE (WITH x -> x + 1 AS test_lambda SELECT test_lambda(1));

SELECT '--';

DESCRIBE (WITH x -> * AS test_lambda SELECT test_lambda(1) AS lambda_value, lambda_value FROM test_table);

SELECT 'Subquery';

DESCRIBE (SELECT (SELECT 1), (SELECT 2), (SELECT 3) AS a, (SELECT 4));

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> (SELECT 1), [1,2,3]), arrayMap(x -> (SELECT 2) AS a, [1, 2, 3]),  arrayMap(x -> (SELECT 1), [1,2,3]));

SELECT '--';

DESCRIBE (SELECT (SELECT 1 AS a, 2 AS b) AS c, c.a, c.b);

SELECT '--';

DESCRIBE (SELECT (SELECT 1 AS a, 2 AS b) AS c, c.*);

SELECT '--';

DESCRIBE (SELECT (SELECT 1 UNION DISTINCT SELECT 1), (SELECT 2 UNION DISTINCT SELECT 2), (SELECT 3 UNION DISTINCT SELECT 3) AS a, (SELECT 4 UNION DISTINCT SELECT 4));

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> (SELECT 1 UNION DISTINCT SELECT 1), [1,2,3]), arrayMap(x -> (SELECT 2 UNION DISTINCT SELECT 2) AS a, [1, 2, 3]),
arrayMap(x -> (SELECT 3 UNION DISTINCT SELECT 3), [1,2,3]));

SELECT '--';

DESCRIBE (SELECT (SELECT 1 AS a, 2 AS b UNION DISTINCT SELECT 1, 2) AS c, c.a, c.b);

SELECT '--';

DESCRIBE (SELECT (SELECT 1 AS a, 2 AS b UNION DISTINCT SELECT 1, 2) AS c, c.*);

SELECT '--';

DESCRIBE (SELECT (SELECT 1), (SELECT 2 UNION DISTINCT SELECT 2), (SELECT 3) AS a, (SELECT 4 UNION DISTINCT SELECT 4));

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> (SELECT 1 UNION DISTINCT SELECT 1), [1,2,3]), arrayMap(x -> (SELECT 2) AS a, [1, 2, 3]),
arrayMap(x -> (SELECT 3 UNION DISTINCT SELECT 3), [1,2,3]));

SELECT 'Window functions';

DESCRIBE (SELECT count() OVER ());

SELECT '--';

DESCRIBE (SELECT count() OVER () AS window_function);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY id) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY id ASC, value DESC ROWS CURRENT ROW) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY id ASC, value DESC ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY id ASC, value DESC RANGE CURRENT ROW) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY id ASC, value DESC RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY (id AS id_alias), (value AS value_alias) ORDER BY id ASC, value DESC ROWS CURRENT ROW) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY (id AS id_alias) ASC, (value AS value_alias) DESC ROWS CURRENT ROW) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY id ASC, value DESC ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY id ASC, value DESC ROWS BETWEEN 1 + 1 PRECEDING AND 2 + 2 FOLLOWING) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (PARTITION BY id, value ORDER BY id ASC, value DESC ROWS BETWEEN ((1 + 1) AS frame_offset_begin) PRECEDING AND ((2 + 2) AS frame_offset_end) FOLLOWING)
FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (ORDER BY toNullable(id) NULLS FIRST) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (ORDER BY toNullable(id) NULLS LAST) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (ORDER BY id WITH FILL FROM 1 TO 5 STEP 1) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (ORDER BY id WITH FILL FROM 1 + 1 TO 6 STEP 1 + 1) FROM test_table);

SELECT '--';

DESCRIBE (SELECT count() OVER (ORDER BY id WITH FILL FROM ((1 + 1) AS from) TO (6 AS to) STEP ((1 + 1) AS step)) FROM test_table);

SELECT 'Window functions WINDOW';

DESCRIBE (SELECT count() OVER window_name FROM test_table WINDOW window_name AS (PARTITION BY id));

SELECT '--';

DESCRIBE (SELECT count() OVER window_name FROM test_table WINDOW window_name AS (PARTITION BY id ORDER BY value));

SELECT '--';

DESCRIBE (SELECT count() OVER (window_name ORDER BY id) FROM test_table WINDOW window_name AS (PARTITION BY id));

SELECT 'IN function';

DESCRIBE (SELECT id IN (SELECT 1) FROM test_table);

SELECT '--';

DESCRIBE (SELECT id IN (SELECT id FROM test_table_in) FROM test_table);

SELECT '--';

DESCRIBE (SELECT id IN test_table_in FROM test_table);

SELECT '--';

DESCRIBE (WITH test_table_in_cte AS (SELECT id FROM test_table) SELECT id IN (SELECT id FROM test_table_in_cte) FROM test_table);

SELECT '--';

DESCRIBE (WITH test_table_in_cte AS (SELECT id FROM test_table) SELECT id IN test_table_in_cte FROM test_table);

SELECT '--';

DESCRIBE (WITH test_table_in_cte_1 AS (SELECT 1 AS c1), test_table_in_cte_2 AS (SELECT 1 AS c1) SELECT *
FROM test_table_in_cte_1 INNER JOIN test_table_in_cte_2 as test_table_in_cte_2 ON test_table_in_cte_1.c1 = test_table_in_cte_2.c1);

SELECT '--';

DESCRIBE (WITH test_table_in_cte_1 AS (SELECT 1 AS c1), test_table_in_cte_2 AS (SELECT 1 AS c1 UNION ALL SELECT 1 AS c1) SELECT *
FROM test_table_in_cte_1 INNER JOIN test_table_in_cte_2 as test_table_in_cte_2 ON test_table_in_cte_1.c1 = test_table_in_cte_2.c1);

SELECT 'Joins';

DESCRIBE (SELECT * FROM test_table_join_1, test_table_join_2);

SELECT '--';

DESCRIBE (SELECT * FROM test_table_join_1 AS t1, test_table_join_2 AS t2);

SELECT '--';

DESCRIBE (SELECT * APPLY toString FROM test_table_join_1 AS t1, test_table_join_2 AS t2);

SELECT '--';

DESCRIBE (SELECT * APPLY x -> toString(x) FROM test_table_join_1 AS t1, test_table_join_2 AS t2);

SELECT '--';

DESCRIBE (SELECT test_table_join_1.*, test_table_join_2.* FROM test_table_join_1 INNER JOIN test_table_join_2 ON test_table_join_1.id = test_table_join_2.id);

SELECT '--';

DESCRIBE (SELECT t1.*, t2.* FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id);

SELECT '--';

DESCRIBE (SELECT test_table_join_1.* APPLY toString, test_table_join_2.* APPLY toString FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id);

SELECT '--';

DESCRIBE (SELECT test_table_join_1.* APPLY x -> toString(x), test_table_join_2.* APPLY x -> toString(x) FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id);

SELECT '--';

DESCRIBE (SELECT test_table_join_1.id, test_table_join_1.value, test_table_join_1.value_join_1, test_table_join_2.id, test_table_join_2.value, test_table_join_2.value_join_2
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id);

SELECT '--';

DESCRIBE (SELECT t1.id, t1.value, t1.value_join_1, t2.id, t2.value, t2.value_join_2 FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id);

SELECT 'Multiple JOINS';

DESCRIBE (SELECT * FROM test_table_join_1, test_table_join_2, test_table_join_3);

SELECT '--';

DESCRIBE (SELECT * FROM test_table_join_1 AS t1, test_table_join_2 AS t2, test_table_join_3 AS t3);

SELECT '--';

DESCRIBE (SELECT * APPLY toString FROM test_table_join_1 AS t1, test_table_join_2 AS t2, test_table_join_3 AS t3);

SELECT '--';

DESCRIBE (SELECT * APPLY x -> toString(x) FROM test_table_join_1 AS t1, test_table_join_2 AS t2, test_table_join_3 AS t3);

SELECT '--';

DESCRIBE (SELECT test_table_join_1.*, test_table_join_2.*, test_table_join_3.*
FROM test_table_join_1 INNER JOIN test_table_join_2 ON test_table_join_1.id = test_table_join_2.id
INNER JOIN test_table_join_3 ON test_table_join_2.id = test_table_join_3.id);

SELECT '--';

DESCRIBE (SELECT t1.*, t2.*, t3.*
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id INNER JOIN test_table_join_3 AS t3 ON t2.id = t3.id);

SELECT '--';

DESCRIBE (SELECT test_table_join_1.* APPLY toString, test_table_join_2.* APPLY toString, test_table_join_3.* APPLY toString
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id INNER JOIN test_table_join_3 AS t3 ON t2.id = t3.id);

SELECT '--';

DESCRIBE (SELECT test_table_join_1.* APPLY x -> toString(x), test_table_join_2.* APPLY x -> toString(x), test_table_join_3.* APPLY x -> toString(x)
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id INNER JOIN test_table_join_3 AS t3 ON t2.id = t3.id);

SELECT '--';

DESCRIBE (SELECT test_table_join_1.id, test_table_join_1.value, test_table_join_1.value_join_1, test_table_join_2.id, test_table_join_2.value, test_table_join_2.value_join_2,
test_table_join_3.id, test_table_join_3.value, test_table_join_3.value_join_3
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id INNER JOIN test_table_join_3 AS t3 ON t2.id = t3.id);

SELECT '--';

DESCRIBE (SELECT t1.id, t1.value, t1.value_join_1, t2.id, t2.value, t2.value_join_2, t3.id, t3.value, t3.value_join_3
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 ON t1.id = t2.id INNER JOIN test_table_join_3 AS t3 ON t2.id = t3.id);

SELECT 'Joins USING';

DESCRIBE (SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id));

SELECT '--';

DESCRIBE (SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id, value));

SELECT '--';

DESCRIBE (SELECT id, t1.id, t1.value, t2.id, t2.value FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id));

SELECT '--';

DESCRIBE (SELECT id, value, t1.id, t1.value, t2.id, t2.value FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id, value));

SELECT 'Multiple Joins USING';

SELECT '--';

DESCRIBE (SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id) INNER JOIN test_table_join_3 AS t3 USING (id));

SELECT '--';

DESCRIBE (SELECT * FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id, value) INNER JOIN test_table_join_3 AS t3 USING (id, value));

SELECT '--';

DESCRIBE (SELECT id, t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id) INNER JOIN test_table_join_3 AS t3 USING (id));

SELECT '--';

DESCRIBE (SELECT id, value, t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM test_table_join_1 AS t1 INNER JOIN test_table_join_2 AS t2 USING (id, value) INNER JOIN test_table_join_3 AS t3 USING (id, value));

SELECT 'Special functions array, tuple';
DESCRIBE (SELECT [], array(), [1], array(1), [1, 2], array(1, 2), tuple(1), (1, 2), [[], []], [([], [])], ([], []), ([([], []), ([], [])]));

-- { echoOff }

DROP TABLE test_table_join_1;
DROP TABLE test_table_join_2;
DROP TABLE test_table_join_3;
DROP TABLE test_table;
DROP TABLE test_table_compound;
