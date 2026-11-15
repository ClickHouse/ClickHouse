DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (number UInt64) ENGINE = MergeTree() ORDER BY number AS SELECT * FROM numbers(2);

SELECT sum(number)
FROM t1
PREWHERE number IN ( SELECT sum(number) FROM t1 GROUP BY number )
   WHERE number IN ( SELECT sum(number) FROM t1 GROUP BY number )
GROUP BY number
ORDER BY number
SETTINGS optimize_syntax_fuse_functions = 1
;
