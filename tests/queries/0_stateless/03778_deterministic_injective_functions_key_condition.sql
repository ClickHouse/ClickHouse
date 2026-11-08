-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

-- { echo }

DROP TABLE IF EXISTS test_deterministic_injective_function_chain;

CREATE TABLE test_deterministic_injective_function_chain
(
    p String
)
ENGINE = MergeTree
ORDER BY reverse(p)
SETTINGS index_granularity = 1;

INSERT INTO test_deterministic_injective_function_chain
SELECT if(number < 9000, 'abc', concat('x', toString(number)))
FROM numbers(10000);

EXPLAIN indexes = 1
SELECT count()
FROM test_deterministic_injective_function_chain
WHERE p = 'abc';

SELECT count()
FROM test_deterministic_injective_function_chain
WHERE p = 'abc';

EXPLAIN indexes = 1
SELECT count()
FROM test_deterministic_injective_function_chain
WHERE p != 'abc';

SELECT count()
FROM test_deterministic_injective_function_chain
WHERE p != 'abc';

EXPLAIN indexes = 1
SELECT count()
FROM test_deterministic_injective_function_chain
WHERE p IN ('abc', 'x9999');

SELECT count()
FROM test_deterministic_injective_function_chain
WHERE p IN ('abc', 'x9999');

EXPLAIN indexes = 1
SELECT count()
FROM test_deterministic_injective_function_chain
WHERE has(['abc', 'x9999'], p);

SELECT count()
FROM test_deterministic_injective_function_chain
WHERE has(['abc', 'x9999'], p);

EXPLAIN indexes = 1
SELECT count()
FROM test_deterministic_injective_function_chain
WHERE p NOT IN ('abc', 'x9999');

SELECT count()
FROM test_deterministic_injective_function_chain
WHERE p NOT IN ('abc', 'x9999');

EXPLAIN indexes = 1
SELECT count()
FROM test_deterministic_injective_function_chain
WHERE NOT has(['abc', 'x9999'], p);

SELECT count()
FROM test_deterministic_injective_function_chain
WHERE NOT has(['abc', 'x9999'], p);

DROP TABLE IF EXISTS test_deterministic_injective_function_dag;

CREATE TABLE test_deterministic_injective_function_dag
(
    p String
)
ENGINE = MergeTree
ORDER BY reverse(tuple(reverse(p), hex(p)))
SETTINGS index_granularity = 1;

INSERT INTO test_deterministic_injective_function_dag
SELECT if(number < 9000, 'abc', concat('x', toString(number)))
FROM numbers(10000);

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag
WHERE p = 'abc';

SELECT count()
FROM test_deterministic_injective_function_dag
WHERE p = 'abc';

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag
WHERE p != 'abc';

SELECT count()
FROM test_deterministic_injective_function_dag
WHERE p != 'abc';

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag
WHERE p IN ('abc', 'x9999');

SELECT count()
FROM test_deterministic_injective_function_dag
WHERE p IN ('abc', 'x9999');

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag
WHERE has(['abc', 'x9999'], p);

SELECT count()
FROM test_deterministic_injective_function_dag
WHERE has(['abc', 'x9999'], p);

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag
WHERE p NOT IN ('abc', 'x9999');

SELECT count()
FROM test_deterministic_injective_function_dag
WHERE p NOT IN ('abc', 'x9999');

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag
WHERE NOT has(['abc', 'x9999'], p);

SELECT count()
FROM test_deterministic_injective_function_dag
WHERE NOT has(['abc', 'x9999'], p);


DROP TABLE IF EXISTS test_deterministic_injective_function_dag_complex;

CREATE TABLE test_deterministic_injective_function_dag_complex
(
    p String
)
ENGINE = MergeTree
ORDER BY reverse(tuple(reverse(lower(p)), hex(lower(p))))
SETTINGS index_granularity = 1;

INSERT INTO test_deterministic_injective_function_dag_complex
SELECT if(number < 9000, 'abc', concat('x', toString(number)))
FROM numbers(10000);

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE lower(p) = 'abc';

SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE lower(p) = 'abc';

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE lower(p) != 'abc';

SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE lower(p) != 'abc';

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE lower(p) IN ('abc', 'x9999');

SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE lower(p) IN ('abc', 'x9999');

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE has(['abc', 'x9999'], lower(p));

SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE has(['abc', 'x9999'], lower(p));

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE lower(p) NOT IN ('abc', 'x9999');

SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE lower(p) NOT IN ('abc', 'x9999');

EXPLAIN indexes=1
SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE NOT has(['abc', 'x9999'], lower(p));

SELECT count()
FROM test_deterministic_injective_function_dag_complex
WHERE NOT has(['abc', 'x9999'], lower(p));
