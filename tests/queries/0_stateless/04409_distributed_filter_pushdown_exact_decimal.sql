-- Tags: no-parallel-replicas

-- Decimal-backed filter constants pushed into a distributed subquery via the predicate-AST pushdown
-- (allow_push_predicate_ast_for_distributed_subqueries, built by tryBuildAdditionalFilterAST) are
-- applied authoritatively on the shard. They must be serialized exactly: a bare numeric literal would
-- be parsed as Float64 and round, dropping the matching rows (and a DateTime64 boundary of 0 used to
-- fail with CANNOT_PARSE_DATETIME, see https://github.com/ClickHouse/ClickHouse/issues/94612).

SET enable_analyzer = 1;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;

-- High-precision DateTime64(9) boundary (19 significant digits, cannot round-trip through Float64).
DROP TABLE IF EXISTS t_04409_dt9;
CREATE TABLE t_04409_dt9 (ts DateTime64(9, 'UTC')) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_04409_dt9 VALUES (fromUnixTimestamp64Nano(1697547086123456789, 'UTC'));

SELECT count()
FROM (SELECT ts FROM remote('127.0.0.{1,2}', currentDatabase(), t_04409_dt9))
WHERE ts = fromUnixTimestamp64Nano(1697547086123456789, 'UTC');

DROP TABLE t_04409_dt9;

-- High-scale Decimal64(5) boundary.
DROP TABLE IF EXISTS t_04409_dec;
CREATE TABLE t_04409_dec (d Decimal64(5)) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_04409_dec VALUES (123456789012.34567);

SELECT count()
FROM (SELECT d FROM remote('127.0.0.{1,2}', currentDatabase(), t_04409_dec))
WHERE d = toDecimal64('123456789012.34567', 5);

DROP TABLE t_04409_dec;

-- The original #94612 boundary (DateTime64 epoch) must not raise CANNOT_PARSE_DATETIME.
DROP TABLE IF EXISTS t_04409_94612;
CREATE TABLE t_04409_94612 (device_id UInt32, data_time DateTime64(3, 'UTC'), data_value UInt64)
ENGINE = MergeTree ORDER BY (device_id, data_time);
INSERT INTO t_04409_94612 VALUES (100, fromUnixTimestamp64Milli(1697547086760), 3), (100, fromUnixTimestamp64Milli(1697547086761), 4);

SELECT count()
FROM (SELECT data_time, max(data_value) FROM remote('127.0.0.{1,2}', currentDatabase(), t_04409_94612) GROUP BY device_id, data_time)
WHERE data_time >= fromUnixTimestamp64Milli(0, 'UTC');

DROP TABLE t_04409_94612;
