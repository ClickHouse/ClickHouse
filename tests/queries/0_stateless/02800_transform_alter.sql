DROP TABLE IF EXISTS test_xy;
DROP TABLE IF EXISTS updates;

CREATE TABLE test_xy
(
    `x` Int32,
    `y` String
)
ENGINE = MergeTree
ORDER BY x;

CREATE TABLE updates
(
    `x` Int32,
    `y` String
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO test_xy(x, y) VALUES (1, 'a1'), (2, 'a2'), (3, 'a3');
INSERT INTO updates(x, y) VALUES  (2, 'b2'), (3, 'b3');

SELECT x, y,
    transform(x,
        (select groupArray(x) from (select x, y from updates order by x) t1),
        (select groupArray(y) from (select x, y from updates order by x) t2),
        y)
FROM test_xy
WHERE 1 ORDER BY x, y;

SET mutations_sync = 1;
ALTER table test_xy
    UPDATE
    y =  transform(x,
        (select groupArray(x) from (select x, y from updates order by x) t1),
        (select groupArray(y) from (select x, y from updates order by x) t2),
        y)
    WHERE 1;

SELECT * FROM test_xy ORDER BY x, y;

DROP TABLE test_xy;
DROP TABLE updates;
