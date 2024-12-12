SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (key UInt8) ENGINE = Memory;
INSERT INTO t1 VALUES (1),(2);

SET join_algorithm = 'full_sorting_merge';

SELECT key FROM ( SELECT key FROM t1 ) AS t1 JOIN ( SELECT key FROM t1 ) AS t2 ON t1.key = t2.key WHERE key;
SELECT key FROM ( SELECT 1 AS key ) AS t1 JOIN ( SELECT 1 AS key ) AS t2 ON t1.key = t2.key WHERE key;
SELECT * FROM ( SELECT 1 AS key GROUP BY NULL ) AS t1 INNER JOIN (SELECT 1 AS key) AS t2 ON t1.key = t2.key WHERE t1.key ORDER BY key;

SET max_rows_in_set_to_optimize_join = 0;

SELECT key FROM ( SELECT key FROM t1 ) AS t1 JOIN ( SELECT key FROM t1 ) AS t2 ON t1.key = t2.key WHERE key;
SELECT key FROM ( SELECT 1 AS key ) AS t1 JOIN ( SELECT 1 AS key ) AS t2 ON t1.key = t2.key WHERE key;
SELECT * FROM ( SELECT 1 AS key GROUP BY NULL ) AS t1 INNER JOIN (SELECT 1 AS key) AS t2 ON t1.key = t2.key WHERE t1.key ORDER BY key;

SET join_algorithm = 'grace_hash';

SELECT * FROM (SELECT key AS a FROM t1 ) t1 INNER JOIN (SELECT key AS c FROM t1 ) t2 ON c = a WHERE a;

DROP TABLE IF EXISTS t1;
