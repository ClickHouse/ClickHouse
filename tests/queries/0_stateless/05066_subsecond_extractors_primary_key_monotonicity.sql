-- https://github.com/ClickHouse/ClickHouse/issues/116944
-- `toMillisecond`, `toMicrosecond` and `toNanosecond` restart at zero every second, so their
-- monotonicity claim to the primary index only holds where both ends of a key range fall into the
-- same second. They used to claim to be monotonic everywhere, and granules whose endpoint images
-- did not bracket the predicate were pruned away.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_millisecond_key;
CREATE TABLE t_millisecond_key (d DateTime64(3)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_millisecond_key VALUES ('2024-01-01 00:00:00.100'), ('2024-01-01 00:00:01.500'), ('2024-01-01 00:00:02.900'), ('2024-01-01 00:00:03.500'), ('2024-01-01 00:00:04.200');

SELECT count(), countIf(toMillisecond(d) = 500) FROM t_millisecond_key WHERE toMillisecond(d) = 500;
SELECT count(), countIf(toMillisecond(d) >= 500) FROM t_millisecond_key WHERE toMillisecond(d) >= 500;
SELECT count() FROM t_millisecond_key WHERE toMillisecond(d) < 500;
SELECT countIf(toMillisecond(d) < 500) FROM t_millisecond_key;

DROP TABLE IF EXISTS t_microsecond_key;
CREATE TABLE t_microsecond_key (d DateTime64(6)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_microsecond_key VALUES ('2024-01-01 00:00:00.000100'), ('2024-01-01 00:00:01.000500'), ('2024-01-01 00:00:02.000900'), ('2024-01-01 00:00:03.000500');

SELECT count(), countIf(toMicrosecond(d) = 500) FROM t_microsecond_key WHERE toMicrosecond(d) = 500;

DROP TABLE IF EXISTS t_nanosecond_key;
CREATE TABLE t_nanosecond_key (d DateTime64(9)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_nanosecond_key VALUES ('2024-01-01 00:00:00.000000100'), ('2024-01-01 00:00:01.000000500'), ('2024-01-01 00:00:02.000000900'), ('2024-01-01 00:00:03.000000500');

SELECT count(), countIf(toNanosecond(d) = 500) FROM t_nanosecond_key WHERE toNanosecond(d) = 500;

-- Within one second the claim is true, so the index is still usable.
SELECT 'inside one second';
SELECT count() FROM t_millisecond_key WHERE d >= '2024-01-01 00:00:01.000' AND d < '2024-01-01 00:00:02.000' AND toMillisecond(d) = 500 SETTINGS force_primary_key = 1;

-- A `DateTime` has no subsecond part, so the extractor is the constant 0 there.
SELECT 'datetime key';
DROP TABLE IF EXISTS t_datetime_key;
CREATE TABLE t_datetime_key (d DateTime) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_datetime_key VALUES ('2024-01-01 00:00:00'), ('2024-01-01 00:00:05');
SELECT count() FROM t_datetime_key WHERE toMillisecond(d) = 0;
SELECT count() FROM t_datetime_key WHERE toMillisecond(d) = 1;

-- A `DateTime64(0)` has no subsecond part either, so the extractors are the constant 0 on it and
-- the index stays usable across second boundaries.
SELECT 'datetime64(0) key';
DROP TABLE IF EXISTS t_datetime64_0_key;
CREATE TABLE t_datetime64_0_key (d DateTime64(0)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_datetime64_0_key VALUES ('2024-01-01 00:00:00'), ('2024-01-01 00:00:01'), ('2024-01-01 00:00:02'), ('2024-01-01 00:00:03');
SELECT count() FROM t_datetime64_0_key WHERE toMillisecond(d) = 0 SETTINGS force_primary_key = 1;
SELECT count() FROM t_datetime64_0_key WHERE toMicrosecond(d) = 0 SETTINGS force_primary_key = 1;
SELECT count() FROM t_datetime64_0_key WHERE toNanosecond(d) = 0 SETTINGS force_primary_key = 1;
SELECT count() FROM t_datetime64_0_key WHERE toMillisecond(d) = 1 SETTINGS force_primary_key = 1;

DROP TABLE t_millisecond_key;
DROP TABLE t_microsecond_key;
DROP TABLE t_nanosecond_key;
DROP TABLE t_datetime_key;
DROP TABLE t_datetime64_0_key;
