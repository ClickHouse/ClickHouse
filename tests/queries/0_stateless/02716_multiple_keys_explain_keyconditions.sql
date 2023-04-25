DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    x UInt32, y UInt32
)
ENGINE = MergeTree
ORDER BY (x, y) settings index_granularity = 1;

INSERT INTO tab SELECT number, number / 3 from numbers(6);

SELECT '---';

SELECT x,y FROM tab;

SELECT '---';

EXPLAIN indexes = 1 SELECT * FROM tab WHERE (x, y) IN ((0, 0), (5, 1));

DROP TABLE tab;