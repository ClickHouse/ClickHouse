SELECT formatDateTime(toDateTime('2023-08-25 15:30:00', 'UTC'), '%H:%i:%S', toLowCardinality('America/Los_Angeles'));
SELECT formatDateTime(toDateTime('2023-08-25 15:30:00', 'UTC'), '%H:%i:%S', toLowCardinality('Europe/Amsterdam'));
SELECT formatDateTime(materialize(toDateTime('2023-08-25 15:30:00', 'UTC')), '%H:%i:%S', toLowCardinality('Europe/Amsterdam'));
SELECT toString(toDateTime('2023-08-25 15:30:00', 'UTC'), toLowCardinality('Europe/Amsterdam'));

-- The cases above only cover a `ColumnConst(ColumnLowCardinality)` time zone. A *non-constant*
-- `LowCardinality(String)` time zone - which is what `if`/`multiIf` produce for constant string
-- branches with `optimize_if_transform_const_strings_to_lowcardinality` - takes the execute-on-
-- dictionary path instead. That path is only correct because `formatDateTime` declares
-- `canBeExecutedOnDefaultArguments` as `false`: the dictionary always carries a default (empty)
-- value, and an empty string is not a valid time zone.

SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SET optimize_if_transform_strings_to_enum = 0;

SELECT 'A non-constant time zone from if/multiIf is LowCardinality(String)';
SELECT toTypeName(if(number % 2 = 0, 'America/Los_Angeles', 'Europe/Amsterdam')) FROM numbers(1);
SELECT toTypeName(multiIf(number % 3 = 0, 'America/Los_Angeles', number % 3 = 1, 'Europe/Amsterdam', 'UTC')) FROM numbers(1);

SELECT 'formatDateTime with a non-constant LowCardinality(String) time zone';
SELECT formatDateTime(
    toDateTime('2023-08-25 15:30:00', 'UTC'),
    '%H:%i:%S',
    if(number % 2 = 0, 'America/Los_Angeles', 'Europe/Amsterdam'))
FROM numbers(2);

SELECT formatDateTime(
    materialize(toDateTime('2023-08-25 15:30:00', 'UTC')),
    '%H:%i:%S',
    multiIf(number % 3 = 0, 'America/Los_Angeles', number % 3 = 1, 'Europe/Amsterdam', 'UTC'))
FROM numbers(3);

SELECT toString(
    toDateTime('2023-08-25 15:30:00', 'UTC'),
    if(number % 2 = 0, 'America/Los_Angeles', 'Europe/Amsterdam'))
FROM numbers(2);

SELECT 'The same results with the optimization disabled (plain String time zone)';
SET optimize_if_transform_const_strings_to_lowcardinality = 0;

SELECT toTypeName(if(number % 2 = 0, 'America/Los_Angeles', 'Europe/Amsterdam')) FROM numbers(1);

SELECT formatDateTime(
    toDateTime('2023-08-25 15:30:00', 'UTC'),
    '%H:%i:%S',
    if(number % 2 = 0, 'America/Los_Angeles', 'Europe/Amsterdam'))
FROM numbers(2);

SELECT 'A constant LowCardinality(String) time zone is accepted by the time-zone arguments';
-- `now` validates its argument in `buildImpl`, which - unlike `getReturnTypeImpl` - is called
-- before the default `LowCardinality` implementation strips the type.
SELECT toTypeName(now(toLowCardinality('UTC')));
SELECT toTypeName(nowInBlock(toLowCardinality('UTC')));
SELECT date_trunc('day', toDateTime('2023-08-25 15:30:00', 'UTC'), toLowCardinality('UTC'));
SELECT toDateTime('2023-08-25 15:30:00', toLowCardinality('UTC'));
SELECT toUnixTimestamp('2023-08-25 15:30:00', toLowCardinality('UTC'));
SELECT toDate('2023-08-25', toLowCardinality('UTC'));
