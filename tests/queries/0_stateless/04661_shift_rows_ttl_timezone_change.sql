-- The fast `MODIFY TTL` path proves the constant TTL delta under the CURRENT column definitions, but a
-- part's stored TTL timestamps were computed under the definitions in effect when it was written. A
-- `DateTime` time zone change is a metadata-only alter (`DataTypeDateTime::equals` ignores the time zone),
-- so without the recorded time zone fingerprint a part written under a DST-observing zone could be shifted
-- by a delta proven under a fixed-offset one, and the shifted bounds would be off by the DST offset.

SET alter_sync = 2;
SET enable_fast_modify_ttl = 1;

DROP TABLE IF EXISTS t_ttl_timezone_change;
CREATE TABLE t_ttl_timezone_change (d DateTime('Europe/Berlin'), v UInt32)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 30 DAY;

-- `2100-03-01 12:00:00` plus 30 days crosses the start of summer time in `Europe/Berlin`, so the stored
-- expiry time is 3600 seconds BELOW the naive `d + 30 * 86400`: `addDays` keeps the local wall-clock time.
INSERT INTO t_ttl_timezone_change SELECT toDateTime('2100-03-01 12:00:00', 'Europe/Berlin'), number FROM numbers(10);

SELECT 'stored bound minus naive shift', toInt64(max(delete_ttl_info_max)) - toInt64(toDateTime('2100-03-01 12:00:00', 'Europe/Berlin')) - 30 * 86400
FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_timezone_change' AND active;

-- A metadata-only time zone change: it converts no data and leaves the part untouched.
ALTER TABLE t_ttl_timezone_change MODIFY COLUMN d DateTime('UTC');
SELECT 'mutations after MODIFY COLUMN', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ttl_timezone_change';

-- `UTC` has a fixed offset, so the delta is provably constant and the table-wide fast path applies, but
-- this part must not be shifted: its bounds have to be recomputed from the data instead.
ALTER TABLE t_ttl_timezone_change MODIFY TTL d + INTERVAL 60 DAY;

-- `UTC` observes no summer time, so the correct bound is exactly `d + 60 * 86400`. A blind shift of the
-- stored bound would keep the 3600 seconds of the `Europe/Berlin` transition and print `-3600` here.
SELECT 'new bound minus naive shift', toInt64(max(delete_ttl_info_max)) - toInt64(toDateTime('2100-03-01 12:00:00', 'Europe/Berlin')) - 60 * 86400
FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_timezone_change' AND active;

SELECT 'rows', count() FROM t_ttl_timezone_change;

DROP TABLE t_ttl_timezone_change;
