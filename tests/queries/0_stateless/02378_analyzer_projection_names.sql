SET use_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

DROP TABLE IF EXISTS test_table_compound;
CREATE TABLE test_table_compound
(
    id UInt64,
    tuple_value Tuple(value_1 UInt64, value_2 String)
) ENGINE=TinyLog;

INSERT INTO test_table_compound VALUES (0, tuple(0, 'Value'));

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

SELECT '--Fix';

DESCRIBE (SELECT test_table.* REPLACE id + (id AS id_alias) AS id, id_alias FROM test_table);

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

SELECT 'Subquery';

DESCRIBE (SELECT (SELECT 1), (SELECT 2), (SELECT 3) AS a, (SELECT 4));

SELECT '--';

DESCRIBE (SELECT arrayMap(x -> (SELECT 1), [1,2,3]), arrayMap(x -> (SELECT 2) AS a, [1, 2, 3]),  arrayMap(x -> (SELECT 1), [1,2,3]));

SELECT '--';

SELECT (SELECT 1 AS a, 2 AS b) AS c, c.a, c.b;

-- { echoOff }

DROP TABLE test_table;
DROP TABLE test_table_compound;
