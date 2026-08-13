DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test2;

SET max_joined_block_size_rows = 5;
SET enable_analyzer = 1;

CREATE TABLE test
(
    c0 Int,
    c1 Date
)
ENGINE = MergeTree()
ORDER BY (c1);

INSERT INTO test (c0, c1) VALUES
(1,'1995-01-28'),
(1,'1995-01-29'),
(1,'1995-01-30');

CREATE TABLE test2
(
    c0 Int,
    c1 Date
)
ENGINE = MergeTree()
ORDER BY (c0);

INSERT INTO test2 (c1, c0) VALUES
('1992-12-14',1),
('1992-12-14',1),
('1989-05-06',1);

SELECT
  count()
FROM test
LEFT JOIN test2
    ON test.c0 = test2.c0
    AND test.c1 >= test2.c1
SETTINGS join_algorithm='parallel_hash';

SELECT
  count()
FROM test2
LEFT JOIN test
    ON test.c0 = test2.c0
    AND test.c1 >= test2.c1
SETTINGS join_algorithm='grace_hash', grace_hash_join_initial_buckets = 2;

SELECT
  count()
FROM test
RIGHT JOIN test2
    ON test.c0 = test2.c0
    AND test.c1 >= test2.c1
SETTINGS join_algorithm='parallel_hash';

SELECT
  count()
FROM test2
RIGHT JOIN test
    ON test.c0 = test2.c0
    AND test.c1 >= test2.c1
SETTINGS join_algorithm='grace_hash', grace_hash_join_initial_buckets = 2;

SELECT
  count()
FROM test
FULL JOIN test2
    ON test.c0 = test2.c0
    AND test.c1 >= test2.c1
SETTINGS join_algorithm='parallel_hash';

SELECT
  count()
FROM test2
FULL JOIN test
    ON test.c0 = test2.c0
    AND test.c1 >= test2.c1
SETTINGS join_algorithm='grace_hash', grace_hash_join_initial_buckets = 2;