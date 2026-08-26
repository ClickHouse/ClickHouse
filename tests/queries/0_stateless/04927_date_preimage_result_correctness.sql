-- The preimage rewrite must not change results when the constant has a different date/time type
-- than the function result, or when a time-zone shift makes two source dates share a day start.

DROP TABLE IF EXISTS t_date_preimage_mixed;
CREATE TABLE t_date_preimage_mixed (ts DateTime('UTC')) ENGINE = Memory;
INSERT INTO t_date_preimage_mixed VALUES ('1970-01-02 12:00:00');

SELECT 'mixed rhs analyzer off', count()
FROM t_date_preimage_mixed
WHERE toDate(ts) = toDateTime(1, 'UTC')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 0;

SELECT 'mixed rhs analyzer on', count()
FROM t_date_preimage_mixed
WHERE toDate(ts) = toDateTime(1, 'UTC')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 1;

SELECT 'mixed rhs legacy off', count()
FROM t_date_preimage_mixed
WHERE toDate(ts) = toDateTime(1, 'UTC')
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 0;

SELECT 'mixed rhs legacy on', count()
FROM t_date_preimage_mixed
WHERE toDate(ts) = toDateTime(1, 'UTC')
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 1;

DROP TABLE t_date_preimage_mixed;

-- Pacific/Apia skipped 2011-12-30, so both dates start at the same timestamp.
DROP TABLE IF EXISTS t_date_preimage_skipped_day;
CREATE TABLE t_date_preimage_skipped_day (d Date) ENGINE = Memory;
INSERT INTO t_date_preimage_skipped_day VALUES ('2011-12-30'), ('2011-12-31');

SELECT 'skipped day analyzer off', arraySort(groupArray(toString(d)))
FROM t_date_preimage_skipped_day
WHERE toStartOfDay(d) = toDateTime('2011-12-31 00:00:00', 'Pacific/Apia')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 0, session_timezone = 'Pacific/Apia';

SELECT 'skipped day analyzer on', arraySort(groupArray(toString(d)))
FROM t_date_preimage_skipped_day
WHERE toStartOfDay(d) = toDateTime('2011-12-31 00:00:00', 'Pacific/Apia')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 1, session_timezone = 'Pacific/Apia';

SELECT 'skipped day legacy off', arraySort(groupArray(toString(d)))
FROM t_date_preimage_skipped_day
WHERE toStartOfDay(d) = toDateTime('2011-12-31 00:00:00', 'Pacific/Apia')
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 0, session_timezone = 'Pacific/Apia';

SELECT 'skipped day legacy on', arraySort(groupArray(toString(d)))
FROM t_date_preimage_skipped_day
WHERE toStartOfDay(d) = toDateTime('2011-12-31 00:00:00', 'Pacific/Apia')
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 1, session_timezone = 'Pacific/Apia';

DROP TABLE t_date_preimage_skipped_day;
