-- Conversion and bitwise functions accept exactly what they accepted before their declarative
-- signatures became authoritative.

-- toInterval* converts from a String, from another interval kind, and from a Dynamic.
SELECT INTERVAL '2' YEAR;
SELECT toIntervalMinute('5');
SELECT toIntervalMinute(toLowCardinality('5'));
SELECT CAST(toIntervalNanosecond(1000) AS IntervalMicrosecond);
SELECT toIntervalDay(CAST(1, 'Dynamic'));
SELECT INTERVAL 1000 NANOSECOND >= INTERVAL 1 MICROSECOND;
-- ... but not from a Decimal, and a DateTime64 is refused by the conversion itself.
SELECT toIntervalDay(toDecimal32(1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toIntervalSecond(now64()); -- { serverError CANNOT_CONVERT_TYPE }

-- bitNot and abs support the 128- and 256-bit integers.
SELECT bitNot(CAST(0, 'UInt128')), toTypeName(bitNot(CAST(0, 'UInt128')));
SELECT bitNot(CAST(0, 'Int256')), toTypeName(bitNot(CAST(0, 'Int256')));
SELECT abs(CAST(-5, 'Int128')), toTypeName(abs(CAST(-5, 'Int128')));
SELECT abs(CAST(-5, 'Int256')), toTypeName(abs(CAST(-5, 'Int256')));

-- toDateTime, toDateTime32 and toDateTime64 inherit the time zone of their argument.
SELECT toTypeName(toDateTime(toDateTime('2020-01-01 00:00:00', 'Europe/Amsterdam')));
SELECT toTypeName(toDateTime32(toDateTime('2020-01-01 00:00:00', 'Europe/Amsterdam')));
SELECT toTypeName(toDateTime64(toDateTime('2020-01-01 00:00:00', 'Europe/Amsterdam'), 3));
SELECT toTypeName(toDateTime64(toDateTime64('2020-01-01 00:00:00', 3, 'Europe/Amsterdam'), 6));
-- An explicit time zone argument still wins.
SELECT toTypeName(toDateTime(toDateTime('2020-01-01 00:00:00', 'Europe/Amsterdam'), 'UTC'));
SELECT toTypeName(toDateTime64(toDateTime('2020-01-01 00:00:00', 'Europe/Amsterdam'), 3, 'UTC'));

-- Time and Time64 carry no time zone, so toTime takes a scale but never a time zone.
SELECT toTypeName(toTime(now(), 3));
SELECT toTime(now(), 'UTC'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A single NULL argument of range makes the whole result NULL, whatever the other arguments are.
SELECT range(NULL), range(10, NULL), range('string', NULL), range(10, 2, NULL);
SELECT range(materialize('string'), NULL);
