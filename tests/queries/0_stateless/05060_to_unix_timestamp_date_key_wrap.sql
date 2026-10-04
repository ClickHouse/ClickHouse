-- `toUnixTimestamp` of a `Date`/`Date32` multiplies the day number by 86400 and wraps the product around
-- `UInt32`, so it is not monotonic over the whole day range. Index analysis must not reorder the endpoints
-- of a key range that reaches the wrapping days.

SET session_timezone = 'UTC';
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_unix_ts_date;
CREATE TABLE t_unix_ts_date (d Date) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_unix_ts_date VALUES ('1970-01-02'), ('2000-01-01'), ('2106-02-06'), ('2149-06-05'), ('2149-06-06');

SELECT count(), (SELECT countIf(toUnixTimestamp(d) >= 2524608000) FROM t_unix_ts_date) FROM t_unix_ts_date WHERE toUnixTimestamp(d) >= 2524608000;
SELECT count(), (SELECT countIf(toUnixTimestamp(d) < 2000000000) FROM t_unix_ts_date) FROM t_unix_ts_date WHERE toUnixTimestamp(d) < 2000000000;
-- A range that stays below the wrapping day keeps its pruning.
SELECT count(), (SELECT countIf(toUnixTimestamp(d) >= 946684800) FROM t_unix_ts_date) FROM t_unix_ts_date WHERE toUnixTimestamp(d) >= 946684800;
SELECT count(), (SELECT countIf(toUnixTimestamp(d) >= 1000000000) FROM t_unix_ts_date) FROM t_unix_ts_date WHERE toUnixTimestamp(d) >= 1000000000;

-- The pruning is real, not just a correct result after a full scan: the granule whose key range
-- stays below the wrapping day is excluded by the index. Without the monotonicity claim for the
-- non-wrapping days nothing could be excluded and this would read all 5 granules.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_unix_ts_date WHERE toUnixTimestamp(d) >= 1000000000 SETTINGS optimize_use_implicit_projections = 0) WHERE explain LIKE '%Granules: %/%';
-- A granule whose key range reaches the wrapping days keeps no claim at all and is always read:
-- only two rows match here, but all 5 granules are kept. Correctness comes first.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_unix_ts_date WHERE toUnixTimestamp(d) < 1000000000 SETTINGS optimize_use_implicit_projections = 0) WHERE explain LIKE '%Granules: %/%';

DROP TABLE IF EXISTS t_unix_ts_date32;
CREATE TABLE t_unix_ts_date32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_unix_ts_date32 VALUES ('1900-01-01'), ('2000-01-01'), ('2100-01-01'), ('2200-01-01'), ('2299-12-31');

SELECT count(), (SELECT countIf(toUnixTimestamp(d) >= 4000000000) FROM t_unix_ts_date32) FROM t_unix_ts_date32 WHERE toUnixTimestamp(d) >= 4000000000;
SELECT count(), (SELECT countIf(toUnixTimestamp(d) < 1000000000) FROM t_unix_ts_date32) FROM t_unix_ts_date32 WHERE toUnixTimestamp(d) < 1000000000;

-- The statistics-based part pruning consults the same claim.
DROP TABLE IF EXISTS t_unix_ts_statistics;
CREATE TABLE t_unix_ts_statistics (d Date) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
ALTER TABLE t_unix_ts_statistics MODIFY COLUMN d Date STATISTICS(basic);
INSERT INTO t_unix_ts_statistics VALUES ('1970-01-02'), ('2000-01-01'), ('2106-02-06'), ('2149-06-05'), ('2149-06-06');

SELECT count(), (SELECT countIf(toUnixTimestamp(d) < 2000000000) FROM t_unix_ts_statistics) FROM t_unix_ts_statistics WHERE toUnixTimestamp(d) < 2000000000;

DROP TABLE t_unix_ts_date;
DROP TABLE t_unix_ts_date32;
DROP TABLE t_unix_ts_statistics;
