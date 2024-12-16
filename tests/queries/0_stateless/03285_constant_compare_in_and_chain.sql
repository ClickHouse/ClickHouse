CREATE TABLE test
(
    `pk` int,
    `a` int,
    `b` int,
    `c` int
)
ENGINE = MergeTree
PRIMARY KEY tuple(pk);

INSERT INTO test values(1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4),(5,5,5,5);

EXPLAIN QUERY TREE
SELECT pk
FROM test
WHERE (a < b) AND (b < c) AND (c < 5);

-- test where condition is constant false
SELECT pk
FROM test
WHERE (a = 3) AND (a = 5);
