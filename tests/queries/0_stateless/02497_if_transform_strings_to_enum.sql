SET allow_experimental_analyzer = 1;
SET optimize_if_transform_strings_to_enum = 1;

SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;
EXPLAIN QUERY TREE run_passes = 1 SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;

SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;
EXPLAIN QUERY TREE run_passes = 1 SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;

SELECT CAST(number > 5 ? '20' : '21', 'Int8') FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT CAST(number > 5 ? '20' : '21', 'Int8') FROM system.numbers LIMIT 10;
EXPLAIN QUERY TREE run_passes = 1 SELECT CAST(number > 5 ? '20' : '21', 'Int8') FROM system.numbers LIMIT 10;

SET optimize_if_transform_strings_to_enum = 0;

SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;
EXPLAIN QUERY TREE run_passes = 1 SELECT transform(number, [2, 4, 6], ['google', 'censor.net', 'yahoo'], 'other') FROM system.numbers LIMIT 10;

SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;
EXPLAIN SYNTAX SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;
EXPLAIN QUERY TREE run_passes = 1 SELECT number > 5 ? 'censor.net' : 'google' FROM system.numbers LIMIT 10;
