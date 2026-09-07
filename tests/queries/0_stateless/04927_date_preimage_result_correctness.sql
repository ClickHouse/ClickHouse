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

-- Wrappers around the constant must not hide its logical type.
DROP TABLE IF EXISTS t_date_preimage_wrapped;
CREATE TABLE t_date_preimage_wrapped (ts DateTime('UTC')) ENGINE = Memory;
INSERT INTO t_date_preimage_wrapped VALUES ('1970-01-02 12:00:00');

SELECT 'lowcardinality rhs analyzer', count()
FROM t_date_preimage_wrapped
WHERE toDate(ts) = toLowCardinality(toDateTime(1, 'UTC'))
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 1;

SELECT 'lowcardinality rhs legacy', count()
FROM t_date_preimage_wrapped
WHERE toDate(ts) = toLowCardinality(toDateTime(1, 'UTC'))
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 1;

SELECT 'nullable rhs legacy', count()
FROM t_date_preimage_wrapped
WHERE toDate(ts) = toNullable(toDateTime(1, 'UTC'))
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 1;

-- The legacy pass runs before type analysis and must not rewrite an invalid comparison.
SELECT count() FROM t_date_preimage_wrapped WHERE toDate(ts) = 1
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_date_preimage_wrapped;

-- Values saturated into the first representable day start share it with the day before.
DROP TABLE IF EXISTS t_date_preimage_lower_edge;
CREATE TABLE t_date_preimage_lower_edge (ts DateTime('Asia/Tokyo'), d Date) ENGINE = Memory;
INSERT INTO t_date_preimage_lower_edge VALUES (0, '1970-01-01'), (54000, '1970-01-02'), (140400, '1970-01-03');

SELECT 'datetime lower edge analyzer', count()
FROM t_date_preimage_lower_edge
WHERE toStartOfDay(ts) = toDateTime(54000, 'UTC')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 1;

SELECT 'datetime lower edge legacy', count()
FROM t_date_preimage_lower_edge
WHERE toStartOfDay(ts) = toDateTime(54000, 'UTC')
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 1;

SELECT 'date lower edge analyzer', arraySort(groupArray(toString(d)))
FROM t_date_preimage_lower_edge
WHERE toStartOfDay(d) = toDateTime(54000, 'UTC')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 1, session_timezone = 'Asia/Tokyo';

SELECT 'date lower edge legacy', arraySort(groupArray(toString(d)))
FROM t_date_preimage_lower_edge
WHERE toStartOfDay(d) = toDateTime(54000, 'UTC')
SETTINGS enable_analyzer = 0, optimize_time_filter_with_preimage = 1, session_timezone = 'Asia/Tokyo';

DROP TABLE t_date_preimage_lower_edge;

-- An implicit-time-zone column keeps its creation time zone, but literals compared against it are
-- parsed in the session one.
DROP TABLE IF EXISTS t_date_preimage_implicit_tz;
CREATE TABLE t_date_preimage_implicit_tz (ts DateTime) ENGINE = Memory SETTINGS session_timezone = 'Pacific/Pago_Pago';
INSERT INTO t_date_preimage_implicit_tz VALUES (toDateTime('2026-02-01 05:00:00', 'UTC'));

SELECT 'implicit tz month not optimized', count()
FROM t_date_preimage_implicit_tz
WHERE toStartOfMonth(ts) = toDate('2026-02-01')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 0, session_timezone = 'Asia/Tokyo';

SELECT 'implicit tz month optimized', count()
FROM t_date_preimage_implicit_tz
WHERE toStartOfMonth(ts) = toDate('2026-02-01')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 1, session_timezone = 'Asia/Tokyo';

SELECT 'implicit tz day not optimized', count()
FROM t_date_preimage_implicit_tz
WHERE toStartOfDay(ts) = toDateTime(1769871600, 'UTC')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 0, session_timezone = 'Asia/Tokyo';

SELECT 'implicit tz day optimized', count()
FROM t_date_preimage_implicit_tz
WHERE toStartOfDay(ts) = toDateTime(1769871600, 'UTC')
SETTINGS enable_analyzer = 1, optimize_time_filter_with_preimage = 1, session_timezone = 'Asia/Tokyo';

DROP TABLE t_date_preimage_implicit_tz;
