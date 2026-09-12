-- Regression test for the monotonicity factor of `toHour`, `toMinute` and `toSecond` over a `DateTime64` key
-- with sub-second precision and pre-epoch values.
--
-- `toHour`, `toMinute` and `toSecond` take the whole part of the `DateTime64`, which `TransformDateTime64`
-- rounds towards negative infinity, but their factor transforms (`toDate`, `toStartOfHour` and
-- `toStartOfMinute`) are dispatched through the `DecimalComponents` overload, which used to truncate towards
-- zero. The two then disagree for a value in the last fractional second before the epoch: the factor puts
-- `1969-12-31 23:59:59.9` in 1970-01-01 / hour 0 / minute 0, the same bucket as the rest of the granule, so
-- the granule is declared monotonic, while the functions themselves return 23 / 59 / 59. The range is then
-- mapped to a descending (empty) one and the granule holding the matching row is pruned.
--
-- The countIf line of each pair is the ground truth (full scan, no key pruning) and must match the count.
-- See also 04307_datetime64_pre_epoch_subsecond_monotonicity, which covers the other dispatch branch.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_factor_subsec;
CREATE TABLE t_factor_subsec (d DateTime64(1, 'UTC')) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- A granule range runs from its own first key to the first key of the next granule, so the first granule
-- spans `1969-12-31 23:59:59.9` to `1970-01-01 00:00:40` - inside one minute, one hour and one day of the
-- factor of every function under test once the left end is truncated towards zero instead of floored.
INSERT INTO t_factor_subsec VALUES ('1969-12-31 23:59:59.9'),('1970-01-01 00:00:10.0'),('1970-01-01 00:00:20.0'),('1970-01-01 00:00:30.0'),('1970-01-01 00:00:40.0'),('1970-01-01 00:00:50.0'),('1970-01-01 00:01:00.0'),('1970-01-01 00:02:00.0');

SELECT count() FROM t_factor_subsec WHERE toHour(d) = 23;
SELECT countIf(toHour(d) = 23) FROM t_factor_subsec;

SELECT count() FROM t_factor_subsec WHERE toMinute(d) = 59;
SELECT countIf(toMinute(d) = 59) FROM t_factor_subsec;

SELECT count() FROM t_factor_subsec WHERE toSecond(d) = 59;
SELECT countIf(toSecond(d) = 59) FROM t_factor_subsec;

DROP TABLE t_factor_subsec;
