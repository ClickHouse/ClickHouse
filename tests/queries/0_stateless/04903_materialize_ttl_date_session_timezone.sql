-- `MergeTreeDataWriter` materializes `Date` bounds in the server time zone, while
-- `ITTLAlgorithm` uses the mutation context's `session_timezone`. A part written with a
-- non-server session zone must therefore fall back from the metadata-only path. Compare it
-- with a packed part, which is always rewritten.

SET alter_sync = 2;
SET session_timezone = 'Asia/Kolkata';

-- This test needs the server's zone and `Asia/Kolkata` to have distinct effective offsets at the
-- date below; otherwise the metadata-only and rewrite paths happen to produce the same bound.
SELECT timezoneOffset(toDateTime('2100-01-01 00:00:00', serverTimeZone())) != timezoneOffset(toDateTime('2100-01-01 00:00:00', 'Asia/Kolkata'));

DROP TABLE IF EXISTS t_ttl_date_timezone_fast;
DROP TABLE IF EXISTS t_ttl_date_timezone_rewrite;

CREATE TABLE t_ttl_date_timezone_fast (d Date)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1000 DAY
SETTINGS min_bytes_for_full_part_storage = 0;

CREATE TABLE t_ttl_date_timezone_rewrite (d Date)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1000 DAY
SETTINGS min_bytes_for_full_part_storage = 1000000000;

INSERT INTO t_ttl_date_timezone_fast SELECT toDate('2100-01-01') FROM numbers(10);
INSERT INTO t_ttl_date_timezone_rewrite SELECT toDate('2100-01-01') FROM numbers(10);

ALTER TABLE t_ttl_date_timezone_fast MODIFY TTL d + INTERVAL 1100 DAY;
ALTER TABLE t_ttl_date_timezone_rewrite MODIFY TTL d + INTERVAL 1100 DAY;

SELECT
    (SELECT max(delete_ttl_info_max) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_date_timezone_fast' AND active)
    =
    (SELECT max(delete_ttl_info_max) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_date_timezone_rewrite' AND active);
SELECT count() FROM t_ttl_date_timezone_fast;
SELECT count() FROM t_ttl_date_timezone_rewrite;

DROP TABLE t_ttl_date_timezone_fast;
DROP TABLE t_ttl_date_timezone_rewrite;
