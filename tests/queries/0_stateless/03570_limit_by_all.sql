SET max_threads = 1;
SET max_insert_threads = 1;
SET max_block_size = 65536;
SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_limit_by_all;

CREATE TABLE test_limit_by_all (
    id Int32,
    category String,
    value Int32,
    name String
) ENGINE = Memory;

INSERT INTO test_limit_by_all VALUES 
(1, 'A', 100, 'item1'),
(1, 'A', 200, 'item2'),
(1, 'B', 300, 'item3'),
(2, 'A', 400, 'item4'),
(2, 'A', 500, 'item5'),
(2, 'B', 600, 'item6'),
(3, 'C', 700, 'item7');

-- Test 1: Basic LIMIT BY ALL usage
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 2: LIMIT BY ALL with aggregate functions (should ignore aggregates)
SELECT id, category, count(*) as cnt
FROM test_limit_by_all 
GROUP BY id, category
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 3: LIMIT BY ALL with computed column - make deterministic by ordering by value
SELECT id, category, concat(category, '_', name) as combined
FROM test_limit_by_all 
ORDER BY id, category, value  -- Order by value to make deterministic when id,category are same
LIMIT 2 BY ALL;

-- Test 4: Basic equivalence test
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 5: Explicit column list (should be equivalent to test 4)
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY id, category, value;

-- Test 6: EXPLAIN SYNTAX test
EXPLAIN SYNTAX 
SELECT id, category, value, rand() AS r
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 7: EXPLAIN QUERY TREE test
EXPLAIN QUERY TREE
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 8: LIMIT BY ALL with window function - make deterministic
SELECT id, category, value, row_number() OVER (PARTITION BY category ORDER BY value) AS rn
FROM test_limit_by_all
ORDER BY id, category, value, rn 
LIMIT 1 BY ALL LIMIT 3;

-- Test 9: LIMIT BY ALL with WHERE clause - make deterministic
SELECT id, category, value 
FROM test_limit_by_all 
WHERE value > 200
ORDER BY id, category, value  -- Add value to ordering for deterministic results
LIMIT 1 BY ALL;

SELECT id AS k, category
FROM test_limit_by_all
ORDER BY k, category, value
LIMIT 1 BY ALL;

SELECT *
FROM test_limit_by_all
ORDER BY id, category, value, name
LIMIT 1 BY ALL;

SELECT id
FROM (SELECT DISTINCT id, category FROM test_limit_by_all)
ORDER BY id, category
LIMIT 1 BY ALL;

SELECT DISTINCT id, category
FROM test_limit_by_all
ORDER BY id, category
LIMIT 1 BY ALL
LIMIT 2;

SELECT d, category, count() AS c
FROM
(
    WITH toStartOfDay(toDateTime('2025-01-01 12:00:00')) AS d
    SELECT d, category
    FROM test_limit_by_all
    ORDER BY d, category, value, name
    LIMIT 2 BY ALL
    SETTINGS enable_positional_arguments = 0
)
GROUP BY d, category
ORDER BY d, category;

SELECT id, category
FROM test_limit_by_all
ORDER BY id, category, value
LIMIT 1,2 BY ALL;

SELECT id, category
FROM test_limit_by_all
ORDER BY id, category, value
LIMIT 2 OFFSET 1 BY ALL;

SELECT id, category
FROM test_limit_by_all
ORDER BY value DESC
LIMIT 1 BY ALL
LIMIT 2;

-- Only-aggregate SELECT -> expect syntax error 62
SELECT count()
FROM test_limit_by_all
LIMIT 1 BY ALL; -- { serverError 62 }

-- JOIN + ARRAY JOIN
DROP TABLE IF EXISTS test_limit_by_all_tags;
CREATE TABLE test_limit_by_all_tags (id Int32, tags Array(String)) ENGINE = Memory;
INSERT INTO test_limit_by_all_tags VALUES (1, ['x','y']), (2, ['y']), (3, ['z']);

EXPLAIN SYNTAX
SELECT t.id, tag, sum(value) AS s
FROM test_limit_by_all AS t
LEFT JOIN test_limit_by_all_tags AS g USING (id)
ARRAY JOIN g.tags AS tag
GROUP BY t.id, tag
ORDER BY t.id, tag
LIMIT 1 BY ALL;

SELECT t.id, tag
FROM test_limit_by_all AS t
LEFT JOIN test_limit_by_all_tags AS g USING (id)
ARRAY JOIN g.tags AS tag
ORDER BY t.id, tag, value
LIMIT 1 BY ALL;

DROP TABLE test_limit_by_all_tags;

WITH toStartOfHour(toDateTime('2025-01-01 12:00:00')) AS h
SELECT h, category
FROM test_limit_by_all
ORDER BY h, category, value
LIMIT 1 BY ALL
SETTINGS enable_positional_arguments = 0;

SELECT id, category, value
FROM test_limit_by_all
ORDER BY id, category, value
LIMIT 1,2 BY id;

SELECT id, category, value
FROM test_limit_by_all
ORDER BY id, category, value
LIMIT 2 OFFSET 1 BY id;

EXPLAIN SYNTAX
SELECT id, category, value
FROM test_limit_by_all
ORDER BY 1, 2, 3
LIMIT 2 BY 1, 2
SETTINGS enable_positional_arguments=1;

SELECT id, category, value
FROM test_limit_by_all
ORDER BY id, category, value
LIMIT -2;

SELECT id, category, value
FROM test_limit_by_all
ORDER BY id, category, value
LIMIT -2 OFFSET -1;

SELECT id, category, value
FROM test_limit_by_all
ORDER BY id, category, value
LIMIT -1 BY id; -- { serverError NOT_IMPLEMENTED }

-- Should give no result
SELECT id, category, value
FROM test_limit_by_all
ORDER BY id, category, value
LIMIT 0 BY ALL;

SELECT id, count() AS c
FROM test_limit_by_all
GROUP BY id, category
HAVING c >= 1
ORDER BY id, c DESC, category
LIMIT 1 BY ALL;

-- NULL key handling
INSERT INTO test_limit_by_all VALUES (4, NULL, 10, 'n1'), (4, NULL, 20, 'n2');

SELECT id, category
FROM test_limit_by_all
ORDER BY id, category NULLS FIRST, value
LIMIT 1 BY ALL;

DROP TABLE test_limit_by_all;