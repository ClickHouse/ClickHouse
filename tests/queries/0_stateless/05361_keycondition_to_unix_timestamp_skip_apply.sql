-- `KeyCondition` keeps the `Field` value instead of executing `toUnixTimestamp(DateTime)` and casts between
-- `DateTime` and unsigned integers on the bounds of a `minmax` index granule. Each filter must prune with the
-- index and return the same result as without it.

DROP TABLE IF EXISTS t_unix_ts_minmax;

CREATE TABLE t_unix_ts_minmax
(
    k UInt64,
    t DateTime('UTC'),
    u UInt32,
    d Date,
    INDEX idx_t t TYPE minmax GRANULARITY 1,
    INDEX idx_u u TYPE minmax GRANULARITY 1,
    INDEX idx_d d TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';

INSERT INTO t_unix_ts_minmax
SELECT number, 1700000000 + number, 1700000000 + number, toDate('2020-01-01') + intDiv(number, 100)
FROM numbers(10000);

SELECT 'toUnixTimestamp(DateTime)';
SELECT count(), min(k) FROM t_unix_ts_minmax WHERE toUnixTimestamp(t) >= 1700009950 SETTINGS force_data_skipping_indices = 'idx_t', max_rows_to_read = 200;
SELECT count(), min(k) FROM t_unix_ts_minmax WHERE toUnixTimestamp(t) >= 1700009950 SETTINGS use_skip_indexes = 0;

SELECT 'toUInt64(DateTime)';
SELECT count(), max(k) FROM t_unix_ts_minmax WHERE toUInt64(t) < 1700000050 SETTINGS force_data_skipping_indices = 'idx_t', max_rows_to_read = 200;
SELECT count(), max(k) FROM t_unix_ts_minmax WHERE toUInt64(t) < 1700000050 SETTINGS use_skip_indexes = 0;

SELECT 'toDateTime(UInt32)';
SELECT count(), min(k) FROM t_unix_ts_minmax WHERE toDateTime(u, 'UTC') > toDateTime(1700009949, 'UTC') SETTINGS force_data_skipping_indices = 'idx_u', max_rows_to_read = 200;
SELECT count(), min(k) FROM t_unix_ts_minmax WHERE toDateTime(u, 'UTC') > toDateTime(1700009949, 'UTC') SETTINGS use_skip_indexes = 0;

-- Converting days to seconds changes the value, so these must still be applied to the bounds.
SELECT 'CAST(Date AS DateTime)';
SELECT count(), min(k) FROM t_unix_ts_minmax WHERE CAST(d AS DateTime('UTC')) >= toDateTime('2020-04-09 00:00:00', 'UTC') SETTINGS force_data_skipping_indices = 'idx_d', max_rows_to_read = 200;
SELECT count(), min(k) FROM t_unix_ts_minmax WHERE CAST(d AS DateTime('UTC')) >= toDateTime('2020-04-09 00:00:00', 'UTC') SETTINGS use_skip_indexes = 0;

SELECT 'toUnixTimestamp(Date)';
SELECT count(), max(k) FROM t_unix_ts_minmax WHERE toUnixTimestamp(d) < 1577923200 SETTINGS force_data_skipping_indices = 'idx_d', max_rows_to_read = 200;
SELECT count(), max(k) FROM t_unix_ts_minmax WHERE toUnixTimestamp(d) < 1577923200 SETTINGS use_skip_indexes = 0;

DROP TABLE t_unix_ts_minmax;
