-- The `Date` conversion in `ITTLAlgorithm` uses the effective `session_timezone` of the
-- mutation context. The fast path must prove and fingerprint the shift in that same zone.
-- Compare a full-storage part (eligible for the metadata-only fast path) with a packed part
-- (which must be rewritten) while a non-empty, non-server `session_timezone` is active.

SET alter_sync = 2;
SET session_timezone = 'Asia/Kolkata';

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
