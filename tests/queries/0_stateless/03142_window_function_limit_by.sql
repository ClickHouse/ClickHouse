SET enable_analyzer = 1;


-- https://github.com/ClickHouse/ClickHouse/issues/55965

CREATE TABLE error_win_func
(
    `k` String,
    `in` UInt64,
    `out` UInt64
)
ENGINE = MergeTree
ORDER BY k AS
SELECT * from VALUES (('a', 2, 4), ('a', 4, 2), ('a', 6, 3), ('a', 8, 4));

SELECT
    k,
    in / out AS ratio,
    count(*) OVER w AS count_rows_w
FROM error_win_func
WINDOW
    w AS (ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING)
LIMIT 1 BY
    k;

DROP TABLE error_win_func;

-- https://github.com/ClickHouse/ClickHouse/issues/47217

CREATE TABLE t(n String, st String) ENGINE = Memory as
select * from values(('a', 'x'), ('b', 'y'), ('c', 'z'));

SELECT
  n as m,
  count() OVER (PARTITION BY m) cnt
FROM t
WHERE st IN ('x', 'y')
LIMIT 1 BY m;

DROP TABLE t;
