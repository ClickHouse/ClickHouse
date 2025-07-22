-- Tests function dateTimeToUUIDv7

-- Basic functionality tests
SELECT toTypeName(dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56')));
SELECT substring(hex(dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'))), 13, 1); -- check version bits (should be '7')
SELECT bitAnd(bitShiftRight(toUInt128(dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'))), 62), 3); -- check variant bits (should be '2')

-- Counter functionality tests (multiple calls with same timestamp)
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56')) = dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'));
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56')) != dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'));

-- Different timestamps should produce different UUIDs
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56')) != dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:57'));

-- Timezone handling
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56', 'UTC')) != dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai'));

-- Error cases
SELECT dateTimeToUUIDv7(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT dateTimeToUUIDv7('2021-08-15 18:57:56'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'), 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Verify timestamp extraction works correctly
SELECT UUIDv7ToDateTime(dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'))) = toDateTime('2021-08-15 18:57:56');

-- Test with constant and non-constant arguments
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'));
SELECT dateTimeToUUIDv7(materialize(toDateTime('2021-08-15 18:57:56')));
