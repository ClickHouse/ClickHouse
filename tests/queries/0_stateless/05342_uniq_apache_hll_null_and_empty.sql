-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- `NULL` values and empty strings are ignored by the Apache DataSketches implementations in Java, Python
-- and C++, and by Spark's `hll_sketch_agg`, so a sketch built here must ignore them too.
-- Unlike `uniq`, an empty string is therefore not counted as a value.

SELECT 'empty strings';
SELECT uniqApacheHLL(x) FROM (SELECT arrayJoin(['', '', '']) AS x);
-- Nothing but empty strings leaves the empty sketch.
SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(['', '']) AS x);
-- The state of 'a' written by the DataSketches C++ library.
SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(['', 'a', '']) AS x) SETTINGS max_threads = 1;
SELECT
    (SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(['a', '', 'b']) AS x))
  = (SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(['a', 'b']) AS x))
SETTINGS max_threads = 1;
-- A fixed-size string of zero bytes is not empty.
SELECT uniqApacheHLL(toFixedString('', 3));

SELECT 'NULL values';
SELECT uniqApacheHLL(x) FROM (SELECT arrayJoin([NULL, 1, NULL, 2, 2]) AS x);
SELECT uniqApacheHLL(x) FROM (SELECT arrayJoin(['', NULL, 'a']) AS x);
SELECT uniqApacheHLL(CAST(NULL, 'Nullable(UInt64)')) FROM numbers(3);
SELECT uniqApacheHLL(toNullable(number)) FROM numbers(0);

SELECT 'a Nullable argument gives the same state as a plain one';
-- No flag byte of the `Null` combinator: the state is the bare DataSketches sketch, also when every row was `NULL`.
SELECT hex(toString(uniqApacheHLLState(toNullable(number)))) FROM numbers(5) SETTINGS max_threads = 1;
SELECT
    (SELECT hex(toString(uniqApacheHLLState(toNullable(number)))) FROM numbers(5))
  = (SELECT hex(toString(uniqApacheHLLState(number))) FROM numbers(5))
SETTINGS max_threads = 1;
SELECT hex(toString(uniqApacheHLLState(toNullable(number)))) FROM numbers(0);
SELECT hex(toString(uniqApacheHLLState(CAST(NULL, 'Nullable(UInt64)')))) FROM numbers(3);
SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(['', NULL]) AS x);
-- NULL rows between values do not change the state.
SELECT
    (SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin([NULL, 1, NULL, 2]) AS x))
  = (SELECT hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin([toNullable(1), 2]) AS x))
SETTINGS max_threads = 1;
-- An external sketch can be imported into a Nullable state type as it is.
SELECT finalizeAggregation(CAST(unhex('1C0201070C03080500CBD7C2042BF2FB06862FF90D7581660781BC5D06'), 'AggregateFunction(uniqApacheHLL, Nullable(UInt64))'));
-- Merging Nullable states, including one built from `NULL` values only.
SELECT uniqApacheHLLMerge(s) FROM
(
    SELECT uniqApacheHLLState(toNullable(number)) AS s FROM numbers(3)
    UNION ALL
    SELECT uniqApacheHLLState(CAST(NULL, 'Nullable(UInt64)')) AS s FROM numbers(2)
);
