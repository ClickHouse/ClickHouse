SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT 'Standalone lambdas';

WITH x -> x + 1 AS lambda SELECT lambda(1);
WITH x -> toString(x) AS lambda SELECT lambda(1), lambda(NULL), lambda([1,2,3]);
WITH x -> toString(x) AS lambda_1, lambda_1 AS lambda_2, lambda_2 AS lambda_3 SELECT lambda_1(1), lambda_2(NULL), lambda_3([1,2,3]);

WITH x -> x + 1 AS lambda SELECT lambda(id) FROM test_table;
WITH x -> toString(x) AS lambda SELECT lambda(id), lambda(value) FROM test_table;

SELECT 'Lambda as function parameter';

SELECT arrayMap(x -> x + 1, [1,2,3]);
WITH x -> x + 1 AS lambda SELECT arrayMap(lambda, [1,2,3]);
SELECT arrayMap((x -> toString(x)) as lambda, [1,2,3]), arrayMap(lambda, ['1','2','3']);
WITH x -> toString(x) AS lambda_1 SELECT arrayMap(lambda_1 AS lambda_2, [1,2,3]), arrayMap(lambda_2, ['1', '2', '3']);

SELECT arrayMap(x -> id, [1,2,3]) FROM test_table;
SELECT arrayMap(x -> x + id, [1,2,3]) FROM test_table;
SELECT arrayMap((x -> concat(concat(toString(x), '_'), toString(id))) as lambda, [1,2,3]) FROM test_table;

SELECT 'Lambda compound argument';

DROP TABLE IF EXISTS test_table_tuple;
CREATE TABLE test_table_tuple
(
    id UInt64,
    value Tuple(value_0_level_0 String, value_1_level_0 String)
) ENGINE=TinyLog;

INSERT INTO test_table_tuple VALUES (0, ('value_0_level_0', 'value_1_level_0'));

WITH x -> concat(concat(toString(x.id), '_'), x.value) AS lambda SELECT cast((1, 'Value'), 'Tuple (id UInt64, value String)') AS value, lambda(value);
WITH x -> concat(concat(x.value_0_level_0, '_'), x.value_1_level_0) AS lambda SELECT lambda(value) FROM test_table_tuple;

SELECT 'Lambda matcher';

WITH x -> * AS lambda SELECT lambda(1);
WITH x -> * AS lambda SELECT lambda(1) FROM test_table;

WITH cast(tuple(1), 'Tuple (value UInt64)') AS compound_value SELECT arrayMap(x -> compound_value.*, [1,2,3]);
WITH cast(tuple(1, 1), 'Tuple (value_1 UInt64, value_2 UInt64)') AS compound_value SELECT arrayMap(x -> compound_value.*, [1,2,3]); -- { serverError UNSUPPORTED_METHOD }
WITH cast(tuple(1, 1), 'Tuple (value_1 UInt64, value_2 UInt64)') AS compound_value SELECT arrayMap(x -> plus(compound_value.*), [1,2,3]);

WITH cast(tuple(1), 'Tuple (value UInt64)') AS compound_value SELECT id, test_table.* APPLY x -> compound_value.* FROM test_table;
WITH cast(tuple(1, 1), 'Tuple (value_1 UInt64, value_2 UInt64)') AS compound_value SELECT id, test_table.* APPLY x -> compound_value.* FROM test_table; -- { serverError UNSUPPORTED_METHOD }
WITH cast(tuple(1, 1), 'Tuple (value_1 UInt64, value_2 UInt64)') AS compound_value SELECT id, test_table.* APPLY x -> plus(compound_value.*) FROM test_table;

SELECT 'Lambda untuple';

WITH x -> untuple(x) AS lambda SELECT cast((1, 'Value'), 'Tuple (id UInt64, value String)') AS value, lambda(value);

SELECT 'Lambda carrying';

WITH (functor, x) -> functor(x) AS lambda, x -> x + 1 AS functor_1, x -> toString(x) AS functor_2 SELECT lambda(functor_1, 1), lambda(functor_2, 1);
WITH (functor, x) -> functor(x) AS lambda, x -> x + 1 AS functor_1, x -> toString(x) AS functor_2 SELECT lambda(functor_1, id), lambda(functor_2, id) FROM test_table;


SELECT 'Lambda legacy syntax';

SELECT arrayMap(lambda(tuple(x), x + 1), [1, 2, 3]);

WITH 222 AS lambda
SELECT arrayMap(lambda(tuple(x), x + 1), [1, 2, 3]);

SELECT arrayMap(lambda((x,), x + 1), [1, 2, 3]);

SELECT arraySort(lambda((x, y), y), ['world', 'hello'], [2, 1]);

WITH 222 AS lambda
SELECT arrayMap(lambda((x, ), x + 1), [1, 2, 3]);

WITH x -> x + 1 AS lambda
SELECT arrayMap(lambda(tuple(x), x + 1), [1, 2, 3]), lambda(1);

-- lambda(tuple(x), x + 1) parsed as lambda definion but not as call of lambda defined in WITH
WITH (x, y) -> y AS lambda
SELECT arrayMap(lambda(tuple(x), x + 1), [1, 2, 3]), lambda(tuple(x), x + 1), 1 AS x; -- { serverError BAD_ARGUMENTS }

WITH (x, y) -> y AS lambda2
SELECT arrayMap(lambda(tuple(x), x + 1), [1, 2, 3]), lambda2(tuple(x), x + 1), 1 AS x;


DROP TABLE test_table_tuple;
DROP TABLE test_table;

WITH x -> (lambda(x) + 1) AS lambda
SELECT lambda(1); -- {serverError UNSUPPORTED_METHOD }

WITH
    x -> (lambda1(x) + 1) AS lambda,
    lambda AS lambda1
SELECT lambda(1); -- {serverError UNSUPPORTED_METHOD }
