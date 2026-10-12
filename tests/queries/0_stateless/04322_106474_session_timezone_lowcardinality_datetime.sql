-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106474
-- `session_timezone` must be applied to `LowCardinality(DateTime)` columns the same
-- way it is applied to plain `DateTime`. The original report described a regression
-- since v26.4 where the dictionary value of `LowCardinality(DateTime)` was rendered
-- with the server's default timezone instead of the session timezone.

SELECT toDateTime(0) AS a,
       toLowCardinality(toDateTime(0)) AS b
SETTINGS session_timezone = 'Asia/Tokyo',
         allow_suspicious_low_cardinality_types = 1
FORMAT TSV;

-- Cover materialized columns and the full table-roundtrip path so the dictionary
-- serializer is exercised, not just the constant-column shortcut.
SELECT materialize(toDateTime(0)) AS a,
       materialize(toLowCardinality(toDateTime(0))) AS b
SETTINGS session_timezone = 'Asia/Tokyo',
         allow_suspicious_low_cardinality_types = 1
FORMAT TSV;

DROP TABLE IF EXISTS t_session_tz_lc_dt;
CREATE TABLE t_session_tz_lc_dt
(
    a DateTime,
    b LowCardinality(DateTime)
) ENGINE = Memory
SETTINGS allow_suspicious_low_cardinality_types = 1;

INSERT INTO t_session_tz_lc_dt VALUES (0, 0);

SELECT * FROM t_session_tz_lc_dt
SETTINGS session_timezone = 'Asia/Tokyo'
FORMAT TSV;

DROP TABLE t_session_tz_lc_dt;
