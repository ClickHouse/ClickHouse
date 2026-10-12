-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- The parameters of `uniqApacheHLL` (`lg_k` and the storage type) do not change what a state means: every sketch carries
-- its own configuration, so states of any parameters can be merged. The types of such states are therefore the same type
-- for the places that need a common type, and not only convertible with `CAST`.

SELECT 'UNION ALL of states with different parameters';
SELECT uniqApacheHLLMerge(s) FROM
(
    SELECT uniqApacheHLLState(8)(toUInt64(number)) AS s FROM numbers(3)
    UNION ALL
    SELECT uniqApacheHLLState(14, 'HLL_6')(toUInt64(number + 10)) AS s FROM numbers(3)
    UNION ALL
    SELECT uniqApacheHLLState(toUInt64(number + 20)) AS s FROM numbers(3)
);

SELECT 'an array of such states has one element type';
SELECT toTypeName([uniqApacheHLLState(8)(toUInt64(1)), uniqApacheHLLState(14, 'HLL_6')(toUInt64(2)), uniqApacheHLLState(toUInt64(3))]) FROM numbers(1);

SELECT 'if between such states';
SELECT toTypeName(if(1, uniqApacheHLLState(8)(number), uniqApacheHLLState(14, 'HLL_6')(number))) FROM numbers(1);

SELECT 'a state with a Nullable argument is the same type as well';
SELECT toTypeName([uniqApacheHLLState(8)(toNullable(toUInt64(1))), uniqApacheHLLState(toUInt64(2))]) FROM numbers(1);

SELECT 'a different argument type is a different type';
SELECT toTypeName([uniqApacheHLLState(8)(toUInt64(1)), uniqApacheHLLState(toString(2))]) FROM numbers(1);

SELECT 'changing the parameters of a column is applied';
DROP TABLE IF EXISTS hll_state_types;
CREATE TABLE hll_state_types (k UInt8, s AggregateFunction(uniqApacheHLL, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO hll_state_types SELECT 1, uniqApacheHLLState(number) FROM numbers(5);
ALTER TABLE hll_state_types MODIFY COLUMN s AggregateFunction(uniqApacheHLL(8), UInt64);
SELECT toTypeName(s), uniqApacheHLLMerge(8)(s) FROM hll_state_types GROUP BY toTypeName(s);
DROP TABLE hll_state_types;
