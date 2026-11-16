set optimize_if_transform_strings_to_enum = 1;

SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;
SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;

set optimize_if_transform_strings_to_enum = 0;

SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;
SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;
