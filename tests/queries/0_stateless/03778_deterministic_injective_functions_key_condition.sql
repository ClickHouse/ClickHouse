-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

DROP TABLE IF EXISTS test_not_equals_injective_function_chain;

CREATE TABLE test_not_equals_injective_function_chain
(
    p String
)
ENGINE = MergeTree
ORDER BY reverse(p)
SETTINGS index_granularity = 1;

INSERT INTO test_not_equals_injective_function_chain
SELECT if(number < 9000, 'abc', concat('x', toString(number)))
FROM numbers(10000);

EXPLAIN indexes = 1
SELECT count()
FROM test_not_equals_injective_function_chain
WHERE p != 'abc';

SELECT count()
FROM test_not_equals_injective_function_chain
WHERE p != 'abc';

DROP TABLE IF EXISTS test_not_equals_injective_function_dag;

CREATE TABLE test_not_equals_injective_function_dag
(
    p String
)
ENGINE = MergeTree
ORDER BY reverse(tuple(reverse(p), hex(p)))
SETTINGS index_granularity = 1;

INSERT INTO test_not_equals_injective_function_dag
SELECT if(number < 9000, 'abc', concat('x', toString(number)))
FROM numbers(10000);

EXPLAIN indexes=1
SELECT count()
FROM test_not_equals_injective_function_dag
WHERE p != 'abc';


SELECT count()
FROM test_not_equals_injective_function_dag
WHERE p != 'abc';


DROP TABLE IF EXISTS test_not_equals_injective_function_dag_complex;

CREATE TABLE test_not_equals_injective_function_dag_complex
(
    p String
)
ENGINE = MergeTree
ORDER BY reverse(tuple(reverse(lower(p)), hex(lower(p))))
SETTINGS index_granularity = 1;

INSERT INTO test_not_equals_injective_function_dag_complex
SELECT if(number < 9000, 'abc', concat('x', toString(number)))
FROM numbers(10000);

EXPLAIN indexes=1
SELECT count()
FROM test_not_equals_injective_function_dag_complex
WHERE lower(p) != 'abc';

SELECT count()
FROM test_not_equals_injective_function_dag_complex
WHERE lower(p) != 'abc';
