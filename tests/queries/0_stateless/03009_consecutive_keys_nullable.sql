DROP TABLE IF EXISTS t_nullable_keys_1;

CREATE TABLE t_nullable_keys_1 (x Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_nullable_keys_1 VALUES (1), (1), (NULL);
SELECT x, count(), countIf(x IS NULL) FROM t_nullable_keys_1 GROUP BY x ORDER BY x;

DROP TABLE t_nullable_keys_1;

DROP TABLE IF EXISTS t_nullable_keys_2;

CREATE TABLE t_nullable_keys_2 (x Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_nullable_keys_2 VALUES (NULL), (1), (1);
SELECT x, count(), countIf(x IS NULL) FROM t_nullable_keys_2 GROUP BY x ORDER BY x;

DROP TABLE t_nullable_keys_2;

DROP TABLE IF EXISTS t_nullable_keys_3;

CREATE TABLE t_nullable_keys_3 (x Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_nullable_keys_3 VALUES (NULL), (NULL), (NULL);
SELECT x, count(), countIf(x IS NULL) FROM t_nullable_keys_3 GROUP BY x ORDER BY x;

DROP TABLE t_nullable_keys_3;

DROP TABLE IF EXISTS t_nullable_keys_4;

CREATE TABLE t_nullable_keys_4 (x Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_nullable_keys_4 VALUES (1), (1), (1);
SELECT x, count(), countIf(x IS NULL) FROM t_nullable_keys_4 GROUP BY x ORDER BY x;

DROP TABLE t_nullable_keys_4;

DROP TABLE IF EXISTS t_nullable_keys_5;

CREATE TABLE t_nullable_keys_5 (x Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_nullable_keys_5 VALUES (1), (NULL), (1);
SELECT x, count(), countIf(x IS NULL) FROM t_nullable_keys_5 GROUP BY x ORDER BY x;

DROP TABLE t_nullable_keys_5;

DROP TABLE IF EXISTS t_nullable_keys_6;

CREATE TABLE t_nullable_keys_6 (x Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_nullable_keys_6 VALUES (NULL), (1), (NULL);
SELECT x, count(), countIf(x IS NULL) FROM t_nullable_keys_6 GROUP BY x ORDER BY x;

DROP TABLE t_nullable_keys_6;

SYSTEM FLUSH LOGS query_log;

SELECT
    splitByChar('.', tables[1])[2] AS table,
    ProfileEvents['AggregationOptimizedEqualRangesOfKeys'] > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE '%SELECT%FROM%t_nullable_keys_%'
ORDER BY table;
