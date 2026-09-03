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

-- A DateTime64 boundary at a DST overlap in a non-UTC time zone: the pushed-down filter must match
-- the exact instant, not the other occurrence that formats to the same local time. The two rows below
-- are distinct UTC instants that both render as 2023-10-29 02:30:00 in Europe/Berlin.
DROP TABLE IF EXISTS t_04409_dstamb;
CREATE TABLE t_04409_dstamb (ts DateTime64(9, 'Europe/Berlin')) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_04409_dstamb VALUES (fromUnixTimestamp64Nano(1698539400000000000, 'Europe/Berlin')), (fromUnixTimestamp64Nano(1698543000000000000, 'Europe/Berlin'));

SELECT DISTINCT toUnixTimestamp64Nano(ts)
FROM (SELECT ts FROM remote('127.0.0.{1,2}', currentDatabase(), t_04409_dstamb))
WHERE ts = fromUnixTimestamp64Nano(1698543000000000000, 'Europe/Berlin');

DROP TABLE t_04409_dstamb;

-- A non-decimal DateTime('Europe/Berlin') boundary at the DST overlap must also be pushed down exactly.
-- The pushed constant keeps its raw Unix-timestamp literal; serializing it as local date-time text would
-- be ambiguous (both rows below render as 2023-10-29 02:30:00) and match the wrong occurrence.
DROP TABLE IF EXISTS t_04409_dt_dst;
CREATE TABLE t_04409_dt_dst (ts DateTime('Europe/Berlin')) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_04409_dt_dst VALUES (1698539400), (1698543000);

SELECT DISTINCT toUnixTimestamp(ts)
FROM (SELECT ts FROM remote('127.0.0.{1,2}', currentDatabase(), t_04409_dt_dst))
WHERE ts = toDateTime(1698543000, 'Europe/Berlin');

DROP TABLE t_04409_dt_dst;

-- The original #94612 boundary (DateTime64 epoch) must not raise CANNOT_PARSE_DATETIME.
DROP TABLE IF EXISTS t_04409_94612;
CREATE TABLE t_04409_94612 (device_id UInt32, data_time DateTime64(3, 'UTC'), data_value UInt64)
ENGINE = MergeTree ORDER BY (device_id, data_time);
INSERT INTO t_04409_94612 VALUES (100, fromUnixTimestamp64Milli(1697547086760), 3), (100, fromUnixTimestamp64Milli(1697547086761), 4);

SELECT count()
FROM (SELECT data_time, max(data_value) FROM remote('127.0.0.{1,2}', currentDatabase(), t_04409_94612) GROUP BY device_id, data_time)
WHERE data_time >= fromUnixTimestamp64Milli(0, 'UTC');

DROP TABLE t_04409_94612;

-- A plain DateTime('Europe/Berlin') leaf carried by a decimal-bearing constant. Its Decimal64 sibling
-- selects the exact serialization of the whole carrier, which re-renders every leaf, so the date-time
-- has to keep its Unix timestamp there too: both rows below render as 2023-10-29 02:30:00, and local
-- text matches the other occurrence. The column is a whole Tuple because an equality between two tuple
-- constructions is split into per-element comparisons, which sends each leaf on its own instead.
DROP TABLE IF EXISTS t_04409_dt_dec;
CREATE TABLE t_04409_dt_dec (tup Tuple(DateTime('Europe/Berlin'), Decimal64(2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04409_dt_dec VALUES ((1698539400, 1.5)), ((1698543000, 1.5));

SELECT DISTINCT toUnixTimestamp(tup.1)
FROM (SELECT tup FROM remote('127.0.0.{1,2}', currentDatabase(), t_04409_dt_dec))
WHERE tup = (toDateTime(1698543000, 'Europe/Berlin'), toDecimal64(1.5, 2))
SETTINGS log_comment = '04409_pushdown_on';

-- The value alone is a blind oracle: a pushdown that declines leaves the filtering to the initiator and
-- returns the right row anyway. This repeats the query with the pushdown off, and the assertion below
-- reads the text each shard was actually sent, which must carry the carrier only in the first case.
SELECT DISTINCT toUnixTimestamp(tup.1)
FROM (SELECT tup FROM remote('127.0.0.{1,2}', currentDatabase(), t_04409_dt_dec))
WHERE tup = (toDateTime(1698543000, 'Europe/Berlin'), toDecimal64(1.5, 2))
SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0, log_comment = '04409_pushdown_off';

SYSTEM FLUSH LOGS query_log;

SELECT countIf(log_comment = '04409_pushdown_on') > 0, countIf(log_comment = '04409_pushdown_off')
FROM system.query_log
WHERE has(databases, currentDatabase()) AND NOT is_initial_query AND type = 'QueryFinish'
  AND query ILIKE '%Tuple(DateTime(%';

DROP TABLE t_04409_dt_dec;
