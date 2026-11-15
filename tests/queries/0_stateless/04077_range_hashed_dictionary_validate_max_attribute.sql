-- Verify that RANGE(MIN ... MAX ...) correctly validates and configures the max attribute.
-- Previously, buildRangeConfiguration used range->min_attr_name for both min and max lookups
-- (copy-paste bug), so a non-existent max attribute was silently accepted.

DROP TABLE IF EXISTS source_04077;
CREATE TABLE source_04077 (id UInt64, start_date Date, end_date Date, value String) ENGINE = Memory;

-- Test 1: Non-existent MAX attribute must be rejected.
DROP DICTIONARY IF EXISTS dict_04077_bad;
CREATE DICTIONARY dict_04077_bad
(
    id UInt64,
    start_date Date,
    end_date Date,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_04077'))
LIFETIME(MIN 0 MAX 100)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX nonexistent_col); -- { serverError INCORRECT_DICTIONARY_DEFINITION }

-- Test 2: Non-existent MIN attribute must still be rejected (regression guard).
DROP DICTIONARY IF EXISTS dict_04077_bad2;
CREATE DICTIONARY dict_04077_bad2
(
    id UInt64,
    start_date Date,
    end_date Date,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_04077'))
LIFETIME(MIN 0 MAX 100)
LAYOUT(RANGE_HASHED())
RANGE(MIN nonexistent_col MAX end_date); -- { serverError INCORRECT_DICTIONARY_DEFINITION }

-- Test 3: Different underlying types for MIN and MAX must be rejected at dictionary creation.
DROP TABLE IF EXISTS source_04077_types;
CREATE TABLE source_04077_types (id UInt64, start_date Date, end_ts DateTime, value String) ENGINE = Memory;

DROP DICTIONARY IF EXISTS dict_04077_type_mismatch;
CREATE DICTIONARY dict_04077_type_mismatch
(
    id UInt64,
    start_date Date,
    end_ts DateTime,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_04077_types'))
LIFETIME(MIN 0 MAX 100)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_ts); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS source_04077_types;

-- Test 4: Nullable types for range attributes are accepted when underlying types match.
-- Previously, the bug caused both range_min and range_max to get the MIN attribute's
-- type, so the Nullable on the max attribute was silently lost.
DROP TABLE IF EXISTS source_04077_nullable;
CREATE TABLE source_04077_nullable (id UInt64, start_date Date, end_date Nullable(Date), value String) ENGINE = Memory;
INSERT INTO source_04077_nullable VALUES (1, '2023-01-01', '2023-12-31', 'hello');

DROP DICTIONARY IF EXISTS dict_04077_nullable;
CREATE DICTIONARY dict_04077_nullable
(
    id UInt64,
    start_date Date,
    end_date Nullable(Date),
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_04077_nullable'))
LIFETIME(MIN 0 MAX 100)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);

SELECT dictGet('dict_04077_nullable', 'value', toUInt64(1), toDate('2023-06-15'));

DROP DICTIONARY IF EXISTS dict_04077_nullable;
DROP TABLE IF EXISTS source_04077_nullable;

-- Test 5: Valid dictionary with matching non-nullable min/max attributes must succeed.
DROP DICTIONARY IF EXISTS dict_04077_good;
CREATE DICTIONARY dict_04077_good
(
    id UInt64,
    start_date Date,
    end_date Date,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_04077'))
LIFETIME(MIN 0 MAX 100)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);

SYSTEM RELOAD DICTIONARY dict_04077_good;
SELECT name, type FROM system.dictionaries WHERE name = 'dict_04077_good' AND database = currentDatabase();

DROP DICTIONARY IF EXISTS dict_04077_good;
DROP TABLE IF EXISTS source_04077;
