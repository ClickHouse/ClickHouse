SELECT groupConcat(',')(arrayJoin(['', '', 'a']));
SELECT groupConcat(',')(arrayJoin(['', '', '']));
SELECT groupConcat(',')(arrayJoin(['', 'a', '']));

SELECT groupConcat(',', 3)(arrayJoin(['', '', 'a']));
SELECT groupConcat(',', 3)(arrayJoin(['', '', '']));

DROP TABLE IF EXISTS t_group_concat_empty_strings;
CREATE TABLE t_group_concat_empty_strings (value String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_group_concat_empty_strings SELECT '' FROM numbers(1000);

SELECT length(groupConcat(',')(value))
FROM t_group_concat_empty_strings
SETTINGS max_threads = 8, max_block_size = 100;

SELECT length(groupConcat(',', 1000)(value))
FROM t_group_concat_empty_strings
SETTINGS max_threads = 8, max_block_size = 100;

DROP TABLE t_group_concat_empty_strings;

DROP TABLE IF EXISTS t_group_concat_unlimited_states;
CREATE TABLE t_group_concat_unlimited_states
(
    state AggregateFunction(groupConcat(','), String)
)
ENGINE = Log;

INSERT INTO t_group_concat_unlimited_states
SELECT groupConcatState(',')('')
FROM numbers(2)
GROUP BY number;

DETACH TABLE t_group_concat_unlimited_states;
ATTACH TABLE t_group_concat_unlimited_states;

SELECT startsWith(toTypeName(state), 'AggregateFunction(1,')
FROM t_group_concat_unlimited_states
LIMIT 1;

SELECT length(groupConcatMerge(',')(state))
FROM t_group_concat_unlimited_states;

TRUNCATE TABLE t_group_concat_unlimited_states;

INSERT INTO t_group_concat_unlimited_states
SELECT groupConcatState(',')(value)
FROM (SELECT CAST('' AS String) AS value WHERE 0);

INSERT INTO t_group_concat_unlimited_states
SELECT groupConcatState(',')('a');

SELECT groupConcatMerge(',')(state)
FROM t_group_concat_unlimited_states;

TRUNCATE TABLE t_group_concat_unlimited_states;

INSERT INTO t_group_concat_unlimited_states
SELECT groupConcatState(',')('');

INSERT INTO t_group_concat_unlimited_states
SELECT groupConcatState(',')('a');

SELECT length(groupConcatMerge(',')(state))
FROM t_group_concat_unlimited_states;

DROP TABLE t_group_concat_unlimited_states;

DROP TABLE IF EXISTS t_group_concat_limited_states;
CREATE TABLE t_group_concat_limited_states
(
    state AggregateFunction(groupConcat(',', 2), String)
)
ENGINE = Log;

INSERT INTO t_group_concat_limited_states
SELECT groupConcatState(',', 2)('')
FROM numbers(2)
GROUP BY number;

SELECT NOT startsWith(toTypeName(state), 'AggregateFunction(1,')
FROM t_group_concat_limited_states
LIMIT 1;

SELECT length(groupConcatMerge(',', 2)(state))
FROM t_group_concat_limited_states;

DROP TABLE t_group_concat_limited_states;

SELECT finalizeAggregation(unhex('0161')::AggregateFunction(groupConcat(','), String));

DROP TABLE IF EXISTS t_group_concat_v0_states;
CREATE TABLE t_group_concat_v0_states
(
    state AggregateFunction(0, groupConcat(','), String)
)
ENGINE = Log;

INSERT INTO t_group_concat_v0_states
SELECT groupConcatState(',')('a');

SELECT groupConcatMerge(',')(state)
FROM t_group_concat_v0_states;

DROP TABLE IF EXISTS t_group_concat_v1_from_v0_states;
CREATE TABLE t_group_concat_v1_from_v0_states
(
    state AggregateFunction(1, groupConcat(','), String)
)
ENGINE = Log;

INSERT INTO t_group_concat_v1_from_v0_states
SELECT state
FROM t_group_concat_v0_states;

SELECT groupConcatMerge(',')(state)
FROM t_group_concat_v1_from_v0_states;

DROP TABLE t_group_concat_v1_from_v0_states;
DROP TABLE t_group_concat_v0_states;

SELECT finalizeAggregation(unhex('016100')::AggregateFunction(1, groupConcat(','), String)); -- { serverError BAD_ARGUMENTS }
SELECT finalizeAggregation(unhex('0002')::AggregateFunction(1, groupConcat(','), String)); -- { serverError BAD_ARGUMENTS }
