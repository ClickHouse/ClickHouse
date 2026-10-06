SET enable_parallel_replicas = 0;
SET enable_analyzer = 1;

CREATE TABLE t1 (key1 UInt64, key2 UInt64, key3 UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 VALUES (11, 12, 13), (21, 22, 23), (31, 32, 33), (41, 42, 43), (51, 52, 53);

CREATE TABLE tj (key2 UInt64, key1 UInt64, key3 UInt64, attr UInt64) ENGINE = Join(ALL, INNER, key3, key2, key1);
INSERT INTO tj VALUES (22, 21, 23, 2000), (32, 31, 33, 3000), (42, 41, 43, 4000), (52, 51, 53, 5000), (62, 61, 63, 6000);

SELECT '============ runtime filters disabled =========';
SET enable_join_runtime_filters = 0;

EXPLAIN
SELECT * FROM t1 ALL INNER JOIN tj USING (key1, key2, key3) ORDER BY key1;

SELECT * FROM t1 ALL INNER JOIN tj USING (key1, key2, key3) ORDER BY key1;

SELECT '============ runtime filters enabled =========';
SET enable_join_runtime_filters = 1;

EXPLAIN
SELECT * FROM t1 ALL INNER JOIN tj USING (key1, key2, key3) ORDER BY key1;

SELECT * FROM t1 ALL INNER JOIN tj USING (key1, key2, key3) ORDER BY key1;
