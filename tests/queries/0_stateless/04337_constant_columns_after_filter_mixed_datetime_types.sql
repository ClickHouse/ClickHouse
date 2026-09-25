SET allow_experimental_time_time64_type = 1;

DROP TABLE IF EXISTS constant_columns_after_filter_mixed_datetime_types;

CREATE TABLE constant_columns_after_filter_mixed_datetime_types
(
    d Date,
    d32 Date32,
    dt DateTime('UTC'),
    dt64 DateTime64(3, 'UTC'),
    tm Time,
    tm64 Time64(3)
)
ENGINE = Memory;

INSERT INTO constant_columns_after_filter_mixed_datetime_types VALUES
(
    toDate('2020-01-01'),
    toDate32('2020-01-01'),
    toDateTime('2020-01-01 00:00:00', 'UTC'),
    toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC'),
    CAST('14:45:40', 'Time'),
    CAST('14:45:40.123', 'Time64(3)')
),
(
    toDate('1970-01-01'),
    toDate32('1970-01-01'),
    toDateTime('1970-01-01 00:00:00', 'UTC'),
    toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC'),
    CAST('00:00:00', 'Time'),
    CAST('00:00:00.000', 'Time64(3)')
);

SELECT 'DateTime = Date', dumpColumnStructure(dt), formatDateTime(dt, '%F %T', 'UTC')
FROM constant_columns_after_filter_mixed_datetime_types
WHERE dt = toDate('2020-01-01')
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'DateTime64 = Date', dumpColumnStructure(dt64), formatDateTime(dt64, '%F %T.%f', 'UTC')
FROM constant_columns_after_filter_mixed_datetime_types
WHERE dt64 = toDate('2020-01-01')
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'Date = DateTime', dumpColumnStructure(d), toString(d)
FROM constant_columns_after_filter_mixed_datetime_types
WHERE d = toDateTime('2020-01-01 00:00:00', 'UTC')
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'Date32 = DateTime64', dumpColumnStructure(d32), toString(d32)
FROM constant_columns_after_filter_mixed_datetime_types
WHERE d32 = toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC')
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'Time = DateTime', dumpColumnStructure(tm), toString(tm)
FROM constant_columns_after_filter_mixed_datetime_types
WHERE tm = toDateTime('1970-01-01 14:45:40', 'UTC')
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'Time64 = DateTime64', dumpColumnStructure(tm64), toString(tm64)
FROM constant_columns_after_filter_mixed_datetime_types
WHERE tm64 = toDateTime64('1970-01-01 14:45:40.123', 3, 'UTC')
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'DateTime = Time', dumpColumnStructure(dt), formatDateTime(dt, '%F %T', 'UTC')
FROM constant_columns_after_filter_mixed_datetime_types
WHERE dt = CAST('00:00:00', 'Time')
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'DateTime64 = Time64', dumpColumnStructure(dt64), formatDateTime(dt64, '%F %T.%f', 'UTC')
FROM constant_columns_after_filter_mixed_datetime_types
WHERE dt64 = CAST('00:00:00.000', 'Time64(3)')
SETTINGS optimize_constant_columns_after_filter = 1;

DROP TABLE constant_columns_after_filter_mixed_datetime_types;
