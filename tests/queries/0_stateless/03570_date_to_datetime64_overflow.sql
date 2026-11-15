SET session_timezone='UTC';

WITH arrayJoin([toDate('1970-01-01'),
                toDate('1970-01-02'),
                toDate('2149-06-05'),
                toDate('2149-06-06')]) AS date
SELECT
    date,
    toUInt16(date)                       AS d16,
    toDateTime64(date, 9)                AS dir_cast,
    CAST(date, 'DateTime64(9)')          AS cast_direct,
    CAST(toDate32(date), 'DateTime64(9)')AS cast_via_date32,
    CAST(toDateTime(date), 'DateTime64(9)') AS cast_via_datetime; -- max value for DateTime is 2106-02-07 06:28:15, so the value overflows
