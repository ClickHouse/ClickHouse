SET allow_experimental_nullable_tuple_type = 1;

SELECT 'CTE Result';
WITH
(
    SELECT tuple('1', toDateTime64('2024-01-01 00:00:00', 6, 'UTC'), 3)
) AS result
SELECT result.1, result.2, result.3;

SELECT 'NULL Nullable(Tuple) element access';
SELECT tupleElement(CAST(NULL AS Nullable(Tuple(Int32, String))), 1);
SELECT toTypeName(tupleElement(CAST(NULL AS Nullable(Tuple(Int32, String))), 1));

SELECT 'Non-NULL Nullable(Tuple) element access';
SELECT tupleElement(CAST((1, 'hello') AS Nullable(Tuple(Int32, String))), 2);
SELECT toTypeName(tupleElement(CAST((1, 'hello') AS Nullable(Tuple(Int32, String))), 2));

SELECT 'Array of Nullable(Tuple) element access - element nullable';
SELECT tupleElement([CAST((1, 'a') AS Nullable(Tuple(Int32, String))), NULL], 1);
SELECT toTypeName(tupleElement([CAST((1, 'a') AS Nullable(Tuple(Int32, String))), NULL], 1));

SELECT 'Array of Nullable(Tuple) element access - element not nullable';
SELECT tupleElement([CAST(([1, 2, 3], 'a') AS Nullable(Tuple(Array(Int32), String))), CAST(NULL AS Nullable(Tuple(Array(Int32), String))), NULL], 1);
SELECT toTypeName(tupleElement([CAST(([1, 2, 3], 'a') AS Nullable(Tuple(Array(Int32), String))), CAST(NULL AS Nullable(Tuple(Array(Int32), String))), NULL], 1));

SELECT 'Error cases for Nullable(Tuple) element access';
SELECT tupleElement(CAST(NULL AS Nullable(Tuple(Int8, String))), 3);-- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

SELECT 'Array of Nullable(Tuple) element access - struct-like access';
SELECT tupleElement(CAST([(1, 7), (3, 4), (2, 8)] AS Array(Nullable(Tuple(x Int8, y Int8)))), 'y');
SELECT toTypeName(tupleElement(CAST([(1, 7), (3, 4), (2, 8)] AS Array(Nullable(Tuple(x Int8, y Int8)))), 'y'));

WITH data AS (
    SELECT [
        CAST((1, 7) AS Nullable(Tuple(x Int8, y Int8))),
        NULL,
        CAST((2, 8) AS Nullable(Tuple(x Int8, y Int8)))
    ] AS arr
)
SELECT tupleElement(arr, 'y') FROM data;

SELECT 'Array of Nullable(Tuple) element access - element already Nullable';
SELECT tupleElement([CAST((1, 'a') AS Nullable(Tuple(Nullable(Int32), String))), NULL], 1);
SELECT toTypeName(tupleElement([CAST((1, 'a') AS Nullable(Tuple(Nullable(Int32), String))), NULL], 1));

SELECT 'Default value when element not found';
SELECT tupleElement(CAST((1, 'hello') AS Nullable(Tuple(Int32, String))), 5, 'default_value');
SELECT toTypeName(tupleElement(CAST((1, 'hello') AS Nullable(Tuple(Int32, String))), 5, 'default_value'));

SELECT 'Default value with NULL tuple';
SELECT tupleElement(CAST(NULL AS Nullable(Tuple(Int32, String))), 1, 999);
SELECT toTypeName(tupleElement(CAST(NULL AS Nullable(Tuple(Int32, String))), 1, 999));

SELECT 'Nested Nullable(Tuple) - outer nullable';
SELECT tupleElement(CAST(((1, 'inner'), 'outer') AS Nullable(Tuple(Tuple(Int32, String), String))), 1);
SELECT toTypeName(tupleElement(CAST(((1, 'inner'), 'outer') AS Nullable(Tuple(Tuple(Int32, String), String))), 1));

SELECT 'Nested Nullable(Tuple) - NULL outer';
SELECT tupleElement(CAST(NULL AS Nullable(Tuple(Tuple(Int32, String), String))), 1);
SELECT toTypeName(tupleElement(CAST(NULL AS Nullable(Tuple(Tuple(Int32, String), String))), 1));

SELECT 'Regular Tuple with Nullable element';
SELECT tupleElement((1, CAST(NULL AS Nullable(String))), 2);
SELECT toTypeName(tupleElement((1, CAST(NULL AS Nullable(String))), 2));

SELECT 'Nested Array of Nullable(Tuple)';
SELECT tupleElement([[CAST((1, 'a') AS Nullable(Tuple(Int32, String))), NULL], [NULL, CAST((2, 'b') AS Nullable(Tuple(Int32, String)))]], 1);
SELECT toTypeName(tupleElement([[CAST((1, 'a') AS Nullable(Tuple(Int32, String))), NULL], [NULL, CAST((2, 'b') AS Nullable(Tuple(Int32, String)))]], 1));

SELECT 'Empty tuple';
SELECT tupleElement(CAST(tuple() AS Nullable(Tuple())), 1); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

SELECT 'All NULL elements in array';
SELECT tupleElement([CAST(NULL AS Nullable(Tuple(Int32, String))), NULL, NULL], 1);
SELECT toTypeName(tupleElement([CAST(NULL AS Nullable(Tuple(Int32, String))), NULL, NULL], 1));

SELECT 'Named access with default value';
SELECT tupleElement(CAST((1, 'hello') AS Nullable(Tuple(x Int32, y String))), 'z', 'default');
SELECT toTypeName(tupleElement(CAST((1, 'hello') AS Nullable(Tuple(x Int32, y String))), 'z', 'default'));

SELECT 'Large tuple access';
SELECT tupleElement(CAST((1, 2, 3, 4, 5, 6, 7, 8, 9, 10) AS Nullable(Tuple(Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32))), 10);
SELECT toTypeName(tupleElement(CAST((1, 2, 3, 4, 5, 6, 7, 8, 9, 10) AS Nullable(Tuple(Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32))), 10));

SELECT 'Const Nullable(Tuple)';
SELECT tupleElement(materialize(CAST((42, 'const') AS Nullable(Tuple(Int32, String)))), 1) FROM numbers(3);
SELECT toTypeName(tupleElement(materialize(CAST((42, 'const') AS Nullable(Tuple(Int32, String)))), 1)) FROM numbers(3);

SELECT 'Multiple elements from same Nullable(Tuple)';
SELECT
    tupleElement(CAST((1, 'hello', 3.14) AS Nullable(Tuple(Int32, String, Float64))), 1) as first,
    tupleElement(CAST((1, 'hello', 3.14) AS Nullable(Tuple(Int32, String, Float64))), 2) as second,
    tupleElement(CAST((1, 'hello', 3.14) AS Nullable(Tuple(Int32, String, Float64))), 3) as third;

SELECT 'Nullable(Tuple) in WHERE clause';
SELECT * FROM (
    SELECT CAST((number, toString(number)) AS Nullable(Tuple(Int32, String))) as t FROM numbers(5)
) WHERE tupleElement(t, 1) > 2;

SELECT 'NULL propagation in expressions';
SELECT tupleElement(CAST(NULL AS Nullable(Tuple(Int32, String))), 1) + 10;


SELECT 'Aggregation on Nullable(Tuple) elements';
SELECT
    sum(tupleElement(t, 1)) as sum_first,
    count(tupleElement(t, 1)) as count_first
FROM (
    SELECT CAST((number, toString(number)) AS Nullable(Tuple(Int32, String))) as t
    FROM numbers(10)
);

SELECT 'Array with alternating NULL/non-NULL tuples';
SELECT tupleElement([
    CAST((1, 'a') AS Nullable(Tuple(Int32, String))),
    NULL,
    CAST((2, 'b') AS Nullable(Tuple(Int32, String))),
    NULL,
    CAST((3, 'c') AS Nullable(Tuple(Int32, String)))
], 2);


DROP TABLE IF EXISTS test_nullable_tuples;
DROP TABLE IF EXISTS test_array_nullable_tuples;
DROP TABLE IF EXISTS test_nullable_named_tuples;
DROP TABLE IF EXISTS test_complex_nullable;

CREATE TABLE test_nullable_tuples
(
    id UInt32,
    data Nullable(Tuple(Int32, String))
) ENGINE = Memory;

INSERT INTO test_nullable_tuples VALUES
    (1, (100, 'hello')),
    (2, NULL),
    (3, (300, 'world')),
    (4, NULL),
    (5, (500, 'test'));

SELECT 'Nullable(Tuple) from table - element 1';
SELECT id, tupleElement(data, 1) as value, toTypeName(tupleElement(data, 1)) as type
FROM test_nullable_tuples
ORDER BY id;

SELECT 'Nullable(Tuple) from table - element 2';
SELECT id, tupleElement(data, 2) as value, toTypeName(tupleElement(data, 2)) as type
FROM test_nullable_tuples
ORDER BY id;

SELECT 'Filter by Nullable(Tuple) element';
SELECT id, tupleElement(data, 1) as value
FROM test_nullable_tuples
WHERE tupleElement(data, 1) > 200
ORDER BY id;

SELECT 'Count non-NULL elements';
SELECT
    count() as total,
    count(tupleElement(data, 1)) as non_null_count,
    countIf(tupleElement(data, 1) IS NULL) as null_count
FROM test_nullable_tuples;

SELECT 'Aggregation on Nullable(Tuple) elements';
SELECT
    sum(tupleElement(data, 1)) as sum_values,
    avg(tupleElement(data, 1)) as avg_values,
    min(tupleElement(data, 1)) as min_value,
    max(tupleElement(data, 1)) as max_value,
    count(tupleElement(data, 1)) as count_non_null
FROM test_nullable_tuples;

CREATE TABLE test_array_nullable_tuples
(
    id UInt32,
    records Array(Nullable(Tuple(Int32, String)))
) ENGINE = Memory;

INSERT INTO test_array_nullable_tuples VALUES
    (1, [CAST((100, 'a') AS Nullable(Tuple(Int32, String))), CAST(NULL AS Nullable(Tuple(Int32, String)))]),
    (2, [CAST((200, 'b') AS Nullable(Tuple(Int32, String))), CAST((300, 'c') AS Nullable(Tuple(Int32, String)))]),
    (3, [CAST(NULL AS Nullable(Tuple(Int32, String)))]),
    (4, []);

SELECT 'Array(Nullable(Tuple)) - extract element 1';
SELECT id, tupleElement(records, 1) as values, toTypeName(tupleElement(records, 1)) as type
FROM test_array_nullable_tuples
ORDER BY id;

SELECT 'Array(Nullable(Tuple)) - extract element 2';
SELECT id, tupleElement(records, 2) as values, toTypeName(tupleElement(records, 2)) as type
FROM test_array_nullable_tuples
ORDER BY id;

SELECT 'Array(Nullable(Tuple)) - unnest and extract';
SELECT id, tupleElement(record, 1) as value, tupleElement(record, 2) as name
FROM test_array_nullable_tuples
ARRAY JOIN records as record
WHERE record IS NOT NULL
ORDER BY id, value;

CREATE TABLE test_nullable_named_tuples
(
    id UInt32,
    person Nullable(Tuple(name String, age UInt8, salary Float64))
) ENGINE = Memory;

INSERT INTO test_nullable_named_tuples VALUES
    (1, ('Alice', 30, 50000.0)),
    (2, NULL),
    (3, ('Charlie', 35, 75000.0)),
    (4, ('Diana', 28, 60000.0)),
    (5, NULL);

SELECT 'Nullable named tuple - by name';
SELECT id,
    tupleElement(person, 'name') as name,
    tupleElement(person, 'age') as age,
    tupleElement(person, 'salary') as salary,
    toTypeName(tupleElement(person, 'name')) as name_type
FROM test_nullable_named_tuples
ORDER BY id;

SELECT 'Nullable named tuple - by index';
SELECT id,
    tupleElement(person, 1) as name,
    tupleElement(person, 2) as age,
    tupleElement(person, 3) as salary
FROM test_nullable_named_tuples
ORDER BY id;

SELECT 'Nullable named tuple - filter by element';
SELECT id, tupleElement(person, 'name') as name, tupleElement(person, 'salary') as salary
FROM test_nullable_named_tuples
WHERE tupleElement(person, 'salary') > 55000
ORDER BY id;

SELECT 'Nullable named tuple - aggregation';
SELECT
    avg(tupleElement(person, 'age')) as avg_age,
    sum(tupleElement(person, 'salary')) as total_salary,
    count(tupleElement(person, 'name')) as count_people
FROM test_nullable_named_tuples;

SELECT 'JOIN using Nullable(Tuple) elements';
SELECT
    t1.id as id1,
    t2.id as id2,
    tupleElement(t1.data, 1) as value1,
    tupleElement(t2.data, 1) as value2
FROM test_nullable_tuples t1
JOIN test_nullable_tuples t2 ON tupleElement(t1.data, 1) = tupleElement(t2.data, 1)
WHERE t1.id < t2.id
ORDER BY t1.id, t2.id;

SELECT 'GROUP BY Nullable(Tuple) element';
SELECT
    tupleElement(data, 2) as category,
    count() as cnt,
    avg(tupleElement(data, 1)) as avg_value
FROM test_nullable_tuples
WHERE data IS NOT NULL
GROUP BY category
ORDER BY category;

SELECT 'Subquery with Nullable(Tuple) element filter';
SELECT id, tupleElement(data, 1) as value
FROM test_nullable_tuples
WHERE id IN (
    SELECT id
    FROM test_nullable_tuples
    WHERE tupleElement(data, 1) > 100
)
ORDER BY id;

SELECT 'CASE on Nullable(Tuple) element';
SELECT
    id,
    CASE
        WHEN tupleElement(data, 1) IS NULL THEN 'null'
        WHEN tupleElement(data, 1) > 200 THEN 'high'
        WHEN tupleElement(data, 1) > 100 THEN 'medium'
        ELSE 'low'
    END as category
FROM test_nullable_tuples
ORDER BY id;

SELECT 'Nullable(Tuple) with default value (element exists)';
SELECT id, tupleElement(data, 1, 999) as value
FROM test_nullable_tuples
ORDER BY id;

SELECT 'Nullable(Tuple) with default value (element does not exist)';
SELECT id, tupleElement(data, 5, 'default_string') as value
FROM test_nullable_tuples
ORDER BY id;

CREATE TABLE test_complex_nullable
(
    id UInt32,
    matrix Array(Array(Nullable(Tuple(x Int32, y Int32))))
) ENGINE = Memory;

INSERT INTO test_complex_nullable VALUES
    (1, [
        [CAST((1, 2) AS Nullable(Tuple(x Int32, y Int32))), CAST(NULL AS Nullable(Tuple(x Int32, y Int32)))],
        [CAST((5, 6) AS Nullable(Tuple(x Int32, y Int32)))]
    ]),
    (2, [
        [CAST((7, 8) AS Nullable(Tuple(x Int32, y Int32)))],
        [CAST(NULL AS Nullable(Tuple(x Int32, y Int32))), CAST((11, 12) AS Nullable(Tuple(x Int32, y Int32)))]
    ]);

SELECT 'Nested array Nullable(Tuple) - extract x values';
SELECT id, tupleElement(matrix, 'x') as x_values, toTypeName(tupleElement(matrix, 'x')) as type
FROM test_complex_nullable
ORDER BY id;

SELECT 'Nested array Nullable(Tuple) - extract y values';
SELECT id, tupleElement(matrix, 'y') as y_values
FROM test_complex_nullable
ORDER BY id;

SELECT 'Window function with Nullable(Tuple) element';
SELECT
    id,
    value,
    row_number() OVER (ORDER BY value NULLS LAST, id)        AS rank,
    dense_rank()  OVER (ORDER BY value NULLS LAST)           AS dense_rank
FROM
(
    SELECT id, tupleElement(data, 1) AS value
    FROM test_nullable_tuples
)
ORDER BY rank;

SELECT 'DISTINCT Nullable(Tuple) elements';
SELECT DISTINCT tupleElement(data, 2) as category
FROM test_nullable_tuples
WHERE tupleElement(data, 2) IS NOT NULL
ORDER BY category;

SELECT 'UNION with Nullable(Tuple) elements';
SELECT value
FROM
(
    SELECT tupleElement(data, 1) AS value FROM test_nullable_tuples WHERE id <= 2
    UNION ALL
    SELECT tupleElement(data, 1) AS value FROM test_nullable_tuples WHERE id >= 4
)
ORDER BY value NULLS LAST;

SELECT 'IN operator with Nullable(Tuple) elements';
SELECT id, tupleElement(data, 1) as value
FROM test_nullable_tuples
WHERE tupleElement(data, 1) IN (100, 300, 500)
ORDER BY id;

SELECT 'ORDER BY Nullable(Tuple) elements';
SELECT id, tupleElement(data, 1) as value, tupleElement(data, 2) as name
FROM test_nullable_tuples
ORDER BY tupleElement(data, 1) NULLS LAST, id;

DROP TABLE IF EXISTS test_nullable_tuples;
DROP TABLE IF EXISTS test_array_nullable_tuples;
DROP TABLE IF EXISTS test_nullable_named_tuples;
DROP TABLE IF EXISTS test_complex_nullable;
