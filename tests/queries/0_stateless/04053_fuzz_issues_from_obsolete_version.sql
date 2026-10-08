SET enable_analyzer = 1;

-- Regression tests for fuzz-found issues reported against obsolete versions.
-- These were fixed by the time of current master.

-- https://github.com/ClickHouse/ClickHouse/issues/100319
CREATE TABLE t_100319 (v1 IPv6, v2 DateTime) ENGINE = Memory;
SELECT (v1 + v2) FROM t_100319; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_100319;

-- https://github.com/ClickHouse/ClickHouse/issues/100321
CREATE TABLE t_100321 (v1 Nullable(Int8), v2 Decimal(18,4)) ENGINE = MergeTree() ORDER BY tuple();
SELECT DISTINCT TOP 1 WITH TIES * FROM t_100321 ORDER BY tuple();
DROP TABLE t_100321;

-- https://github.com/ClickHouse/ClickHouse/issues/100322
CREATE TABLE t_100322 (v1 Int8, v8 Int64) ENGINE = Memory;
ALTER TABLE t_100322 DROP COLUMN IF EXISTS v1;
ALTER TABLE t_100322 COMMENT COLUMN IF EXISTS v1 'test';
DROP TABLE t_100322;

-- https://github.com/ClickHouse/ClickHouse/issues/100323
CREATE TABLE t_100323 (v1 Enum8('D' = 1, 'A' = 2, 'C' = 3), v2 Decimal(18,4), v3 IPv4, v4 DateTime, v5 DateTime, v6 FixedString(16), v7 Nullable(String), v8 Int8) ENGINE = Memory;
SELECT DISTINCT CASE WHEN 1 THEN t_100323.v1 AND t_100323.v2 WHEN INTERVAL 1 DAY THEN t_100323.v1 == t_100323.v1 WHEN t_100323.v1 * t_100323.v1 THEN CAST(t_100323.v1 AS UInt8) ELSE t_100323.v1 END alias FROM t_100323 LEFT ARRAY JOIN *, (SELECT t_100323.v1 FROM t_100323 UNION ALL SELECT t_100323.v2 FROM t_100323 UNION ALL (SELECT t_100323.v1 FROM t_100323) UNION ALL SELECT t_100323.v1 FROM t_100323) WITH CUBE ORDER BY t_100323.v1 ASCENDING NULLS LAST, t_100323.v2 LIMIT 1, 1; -- { serverError TYPE_MISMATCH }
DROP TABLE t_100323;

-- https://github.com/ClickHouse/ClickHouse/issues/100326
WITH interval AS (SELECT 1 AS val), t0_renamed AS (SELECT * FROM t0_renamed) SELECT TOP 10 *, t0_renamed.*, *, t0_renamed.* WHERE -t0_renamed.v1 GROUP BY t0_renamed.v1, t0_renamed.v2, t0_renamed.v3; -- { serverError UNKNOWN_IDENTIFIER }

-- https://github.com/ClickHouse/ClickHouse/issues/100327
CREATE TABLE t_100327_0 (v1 UInt16, v2 FixedString(16), v3 LowCardinality(String), v4 Int8, v5 UInt64, v6 String, v7 Enum16('A' = 1, 'C' = 2, 'E' = 3)) ENGINE = TinyLog;
CREATE TABLE t_100327_1 (v1 UInt8, v2 DateTime, v3 Nullable(Enum16('C' = 1, 'A' = 2, 'D' = 3)), v4 Enum16('C' = 1, 'B' = 2, 'D' = 3), v5 LowCardinality(String)) ENGINE = Memory;
CREATE TABLE t_100327_2 (v1 Nullable(UInt8), v2 UInt64, v3 Enum16('D' = 1, 'E' = 2, 'B' = 3, 'C' = 4), v4 Int64, v5 UInt8, v6 Nullable(UInt32), v7 Nullable(UInt64)) ENGINE = TinyLog;
SELECT t_100327_0.v1, t_100327_1.v2, t_100327_2.v3 FROM t_100327_0 JOIN t_100327_1 USING (v1) JOIN t_100327_2 USING (v1) WHERE t_100327_0.v1 > 100;
DROP TABLE t_100327_0;
DROP TABLE t_100327_1;
DROP TABLE t_100327_2;

-- https://github.com/ClickHouse/ClickHouse/issues/100329
CREATE TABLE t_100329 (v1 DateTime64(3), v2 Int16, v3 Nullable(DateTime64(3)), v4 Nullable(DateTime64(3)), v5 Nullable(UInt8), v6 LowCardinality(String), v7 Float64) ENGINE = SummingMergeTree() ORDER BY v2;
INSERT INTO t_100329 (v1, v2, v3, v4, v5, v6, v7) VALUES (toDateTime('2001-09-18 10:03:30'), 17349, toDateTime('2013-07-02 01:02:55'), toDateTime('1978-05-17 09:23:31'), NULL, 'bx3t6', inf);
ALTER TABLE t_100329 MODIFY COLUMN v2 COMMENT 'Primary key column';
ALTER TABLE t_100329 MODIFY TTL toStartOfDay(v1) + INTERVAL 1 DAY GROUP BY v2;
DROP TABLE t_100329;
