CREATE TABLE example
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 8192;

INSERT INTO example SELECT * FROM generateRandom('a UInt64,\n    b String', 1, 10, 2) LIMIT 1;

SELECT a, b FROM example ORDER BY a WITH FILL FROM 1 TO 100 INTERPOLATE (b, b) -- { serverError BAD_ARGUMENTS }
