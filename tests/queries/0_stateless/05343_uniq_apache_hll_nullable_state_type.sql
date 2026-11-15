-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- The state of a `Nullable` argument is the same bare DataSketches sketch as the state of a plain one,
-- so it also has the same type, and the two can be mixed without a `CAST`.

SELECT 'the state type does not depend on the nullability of the argument';
SELECT toTypeName(uniqApacheHLLState(toNullable(number))) FROM numbers(1);
SELECT toTypeName(uniqApacheHLLState(number)) FROM numbers(1);
SELECT toTypeName(uniqApacheHLLState(14, 'HLL_8')(toNullable(toString(number)))) FROM numbers(1);

SELECT 'LowCardinality is stripped from the arguments of aggregate functions';
SELECT toTypeName(uniqApacheHLLState(toLowCardinality(toNullable(toString(number))))) FROM numbers(1);
SELECT uniqApacheHLL(x) FROM (SELECT arrayJoin(CAST(['a', NULL, 'b', 'a', ''], 'Array(LowCardinality(Nullable(String)))')) AS x);
SELECT
    (SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['a', NULL, 'b'], 'Array(LowCardinality(Nullable(String)))')) AS x))
  = (SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(['a', 'b']) AS x))
SETTINGS max_threads = 1;

SELECT 'states of Nullable and plain arguments mix';
SELECT uniqApacheHLLMerge(s) FROM
(
    SELECT uniqApacheHLLState(toNullable(number)) AS s FROM numbers(3)
    UNION ALL
    SELECT uniqApacheHLLState(number + 3) AS s FROM numbers(3)
);

SELECT 'the If combinator over Nullable arguments keeps the same state and type';
SELECT toTypeName(uniqApacheHLLStateIf(toNullable(number), number % 2 = 0)) FROM numbers(1);
SELECT toTypeName(uniqApacheHLLStateIf(number, number % 2 = 0)) FROM numbers(1);
SELECT toTypeName(uniqApacheHLLStateIf(number, toNullable(number % 2 = 0))) FROM numbers(1);
-- The state is the bare sketch of the values 0, 2 and 4, without a flag byte.
SELECT
    (SELECT hex(toString(uniqApacheHLLStateIf(toNullable(number), number % 2 = 0))) FROM numbers(5))
  = (SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin([0, 2, 4]) AS x))
SETTINGS max_threads = 1;
SELECT
    (SELECT hex(toString(uniqApacheHLLStateIf(number, toNullable(number % 2 = 0)))) FROM numbers(5))
  = (SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin([0, 2, 4]) AS x))
SETTINGS max_threads = 1;
SELECT uniqApacheHLLIf(toNullable(number), number % 2 = 0) FROM numbers(10);
SELECT uniqApacheHLLMergeIf(s, c) FROM (SELECT uniqApacheHLLState(number) AS s, number % 2 = 0 AS c FROM numbers(10) GROUP BY number);
SELECT uniqApacheHLLMergeIf(s, toNullable(c)) FROM (SELECT uniqApacheHLLState(number) AS s, number % 2 = 0 AS c FROM numbers(10) GROUP BY number);

SELECT 'a declared Nullable state type is interchangeable with the plain one';
SELECT toTypeName(CAST(uniqApacheHLLState(number), 'AggregateFunction(uniqApacheHLL, Nullable(UInt64))')) FROM numbers(1);
SELECT finalizeAggregation(CAST(uniqApacheHLLState(number), 'AggregateFunction(uniqApacheHLL, Nullable(UInt64))')) FROM numbers(4);
SELECT finalizeAggregation(CAST(CAST(uniqApacheHLLState(number), 'AggregateFunction(uniqApacheHLL, Nullable(UInt64))'), 'AggregateFunction(uniqApacheHLL, UInt64)')) FROM numbers(4);

SELECT 'tables';
DROP TABLE IF EXISTS hll_plain_states;
CREATE TABLE hll_plain_states (k UInt8, s AggregateFunction(uniqApacheHLL, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO hll_plain_states SELECT 0 AS k, uniqApacheHLLState(toNullable(number)) FROM numbers(3);
INSERT INTO hll_plain_states SELECT 0 AS k, uniqApacheHLLState(number + 3) FROM numbers(3);
SELECT uniqApacheHLLMerge(s) FROM hll_plain_states;
DROP TABLE hll_plain_states;

DROP TABLE IF EXISTS hll_nullable_states;
CREATE TABLE hll_nullable_states (k UInt8, s AggregateFunction(uniqApacheHLL, Nullable(UInt64))) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO hll_nullable_states SELECT 0 AS k, uniqApacheHLLState(toNullable(number)) FROM numbers(3);
INSERT INTO hll_nullable_states SELECT 0 AS k, uniqApacheHLLState(number + 3) FROM numbers(3);
SELECT uniqApacheHLLMerge(s) FROM hll_nullable_states;
DROP TABLE hll_nullable_states;
