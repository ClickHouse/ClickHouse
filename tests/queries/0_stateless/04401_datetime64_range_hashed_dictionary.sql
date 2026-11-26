-- Tests that a range_hashed / complex_key_range_hashed dictionary can be queried with a
-- DateTime64 range argument (and that sub-second precision is preserved), as well as with a
-- Decimal range argument. Previously dictGet/dictHas rejected such arguments with
-- "must be convertible to Int64", even though DateTime64/Decimal are valid range column types.
-- https://github.com/ClickHouse/ClickHouse/issues/46060

DROP TABLE IF EXISTS 04401_range_source;
CREATE TABLE 04401_range_source
(
    offer_id UInt64,
    postback_status UInt64,
    date_start DateTime64(6, 'UTC'),
    date_end Nullable(DateTime64(6, 'UTC')),
    amount Decimal(12, 2)
)
ENGINE = TinyLog;

INSERT INTO 04401_range_source VALUES
    (4, 5, '2018-11-28 16:10:05.636720', '2019-04-01 00:00:00.451254', 50),
    (4, 5, '2019-04-01 00:00:00.451254', '2019-11-30 23:59:59.000000', 100),
    (4, 4, '2019-11-30 23:59:59.000000', NULL, 250);

DROP DICTIONARY IF EXISTS 04401_range_dict;
CREATE DICTIONARY 04401_range_dict
(
    offer_id UInt64,
    postback_status UInt64,
    date_start DateTime64(6, 'UTC'),
    date_end Nullable(DateTime64(6, 'UTC')),
    amount Decimal(12, 2)
)
PRIMARY KEY offer_id, postback_status
SOURCE(CLICKHOUSE(TABLE '04401_range_source'))
LAYOUT(COMPLEX_KEY_RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN date_start MAX date_end)
LIFETIME(0);

SELECT 'DateTime64 range argument, open-ended (NULL) interval';
-- A point far in the future falls into the open-ended interval of (4, 4) -> 250.
SELECT dictGet('04401_range_dict', 'amount', (toUInt64(4), toUInt64(4)), toDateTime64('2025-01-01 00:00:00.000000', 6, 'UTC'));
-- The open interval starts exactly at its lower bound -> 250.
SELECT dictGet('04401_range_dict', 'amount', (toUInt64(4), toUInt64(4)), toDateTime64('2019-11-30 23:59:59.000000', 6, 'UTC'));

SELECT 'DateTime64 range argument, sub-second precision';
-- Inside the second closed interval of (4, 5) -> 100.
SELECT dictGet('04401_range_dict', 'amount', (toUInt64(4), toUInt64(5)), toDateTime64('2019-08-08 23:59:59.636720', 6, 'UTC'));
-- Exactly at the lower bound of the first interval of (4, 5) -> 50.
SELECT dictGet('04401_range_dict', 'amount', (toUInt64(4), toUInt64(5)), toDateTime64('2018-11-28 16:10:05.636720', 6, 'UTC'));
-- One microsecond before that lower bound -> outside any interval -> default 0.
SELECT dictGet('04401_range_dict', 'amount', (toUInt64(4), toUInt64(5)), toDateTime64('2018-11-28 16:10:05.636719', 6, 'UTC'));
-- Overlapping point shared by both (4, 5) intervals: range_lookup_strategy 'max' picks the
-- interval with the larger range_min -> the second one -> 100.
SELECT dictGet('04401_range_dict', 'amount', (toUInt64(4), toUInt64(5)), toDateTime64('2019-04-01 00:00:00.451254', 6, 'UTC'));

SELECT 'dictHas with DateTime64 range argument';
SELECT dictHas('04401_range_dict', (toUInt64(4), toUInt64(4)), toDateTime64('2025-01-01 00:00:00.000000', 6, 'UTC'));
SELECT dictHas('04401_range_dict', (toUInt64(4), toUInt64(5)), toDateTime64('2018-11-28 16:10:05.636719', 6, 'UTC'));

DROP DICTIONARY 04401_range_dict;
DROP TABLE 04401_range_source;

SELECT 'Simple key with DateTime64 range';

DROP TABLE IF EXISTS 04401_range_source_simple;
CREATE TABLE 04401_range_source_simple
(
    id UInt64,
    date_start DateTime64(3, 'UTC'),
    date_end Nullable(DateTime64(3, 'UTC')),
    value String
)
ENGINE = TinyLog;

INSERT INTO 04401_range_source_simple VALUES
    (1, '2020-01-01 00:00:00.100', '2020-06-01 00:00:00.500', 'first'),
    (1, '2020-06-01 00:00:00.500', NULL, 'open');

DROP DICTIONARY IF EXISTS 04401_range_dict_simple;
CREATE DICTIONARY 04401_range_dict_simple
(
    id UInt64,
    date_start DateTime64(3, 'UTC'),
    date_end Nullable(DateTime64(3, 'UTC')),
    value String DEFAULT 'none'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '04401_range_source_simple'))
LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN date_start MAX date_end)
LIFETIME(0);

SELECT dictGet('04401_range_dict_simple', 'value', toUInt64(1), toDateTime64('2020-03-01 00:00:00.000', 3, 'UTC'));
SELECT dictGet('04401_range_dict_simple', 'value', toUInt64(1), toDateTime64('2030-01-01 00:00:00.000', 3, 'UTC'));
SELECT dictGet('04401_range_dict_simple', 'value', toUInt64(1), toDateTime64('2020-01-01 00:00:00.099', 3, 'UTC'));

DROP DICTIONARY 04401_range_dict_simple;
DROP TABLE 04401_range_source_simple;

SELECT 'Decimal range argument';

DROP TABLE IF EXISTS 04401_range_source_decimal;
CREATE TABLE 04401_range_source_decimal
(
    id UInt64,
    range_start Decimal(18, 6),
    range_end Decimal(18, 6),
    value String
)
ENGINE = TinyLog;

INSERT INTO 04401_range_source_decimal VALUES
    (1, 0.000000, 10.500000, 'low'),
    (1, 10.500000, 100.000000, 'high');

DROP DICTIONARY IF EXISTS 04401_range_dict_decimal;
CREATE DICTIONARY 04401_range_dict_decimal
(
    id UInt64,
    range_start Decimal(18, 6),
    range_end Decimal(18, 6),
    value String DEFAULT 'none'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '04401_range_source_decimal'))
LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN range_start MAX range_end)
LIFETIME(0);

SELECT dictGet('04401_range_dict_decimal', 'value', toUInt64(1), toDecimal64(5.25, 6));
SELECT dictGet('04401_range_dict_decimal', 'value', toUInt64(1), toDecimal64(50.0, 6));

DROP DICTIONARY 04401_range_dict_decimal;
DROP TABLE 04401_range_source_decimal;

SELECT 'Float range argument';

DROP TABLE IF EXISTS 04401_range_source_float;
CREATE TABLE 04401_range_source_float
(
    id UInt64,
    range_start Float64,
    range_end Nullable(Float64),
    value String
)
ENGINE = TinyLog;

INSERT INTO 04401_range_source_float VALUES
    (1, 0.0, 10.5, 'low'),
    (1, 10.5, NULL, 'high');

DROP DICTIONARY IF EXISTS 04401_range_dict_float;
CREATE DICTIONARY 04401_range_dict_float
(
    id UInt64,
    range_start Float64,
    range_end Nullable(Float64),
    value String DEFAULT 'none'
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '04401_range_source_float'))
LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN range_start MAX range_end)
LIFETIME(0);

-- A Float64 range column is accepted by range_hashed, so a fractional Float64 argument must be
-- accepted by dictGet as well (it is cast to the dictionary's range type before the lookup).
SELECT dictGet('04401_range_dict_float', 'value', toUInt64(1), toFloat64(5.25));
-- A point far in the future falls into the open-ended (NULL max) interval.
SELECT dictGet('04401_range_dict_float', 'value', toUInt64(1), toFloat64(1000.0));

SELECT 'Unsupported range argument types are rejected with a clean error';
-- A variable-size type such as String must produce ILLEGAL_COLUMN, not a LOGICAL_ERROR.
SELECT dictGet('04401_range_dict_float', 'value', toUInt64(1), 'not a number'); -- { serverError ILLEGAL_COLUMN }
-- A wider-than-Int64 numeric argument is rejected, since the range value is compared as Int64.
SELECT dictGet('04401_range_dict_float', 'value', toUInt64(1), toInt128(5)); -- { serverError ILLEGAL_COLUMN }

DROP DICTIONARY 04401_range_dict_float;
DROP TABLE 04401_range_source_float;
