-- deterministic test for LIMIT BY ALL
SET max_threads = 1;
SET max_insert_threads = 1;
SET max_block_size = 65536;

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

SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;


SELECT id, category, count(*) as cnt
FROM test_limit_by_all 
GROUP BY id, category
ORDER BY id, category 
LIMIT 1 BY ALL;

SELECT id, category, concat(category, '_', name) as combined
FROM test_limit_by_all 
ORDER BY id, category 
LIMIT 2 BY ALL;

SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY id, category, value;

EXPLAIN SYNTAX 
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

EXPLAIN QUERY TREE
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

SELECT id, category, value, row_number() OVER (PARTITION BY category ORDER BY value) AS rn
FROM test_limit_by_all
ORDER BY id, category, value, rn 
LIMIT 1 BY ALL;

SELECT id, category, value 
FROM test_limit_by_all 
WHERE value > 200
ORDER BY id, category 
LIMIT 1 BY ALL;

DROP TABLE test_limit_by_all;