-- Test multi-argument uniq/uniqExact/uniqHLL12 with the -If combinator in aggregation without key.
-- Multi-arg (variadic) uniqIf routes to AggregateFunctionUniqVariadic::addBatchSinglePlace /
-- addBatchSinglePlaceNotNull, which read the -If flags column. The filtered result must equal
-- the same aggregate evaluated over a WHERE-filtered copy of the data.
DROP TABLE IF EXISTS t_uniq_variadic_if;

CREATE TABLE t_uniq_variadic_if
(
    a UInt64,
    b String,
    na Nullable(UInt64),
    cond UInt8
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_uniq_variadic_if
SELECT number % 50, toString(number % 30), if(number % 7 = 0, NULL, number % 40), number % 3 = 0
FROM numbers(20000);

-- Non-nullable arguments: exercises addBatchSinglePlace with -If flags.
SELECT
    uniqExactIf(a, b, cond) = (SELECT uniqExact(a, b) FROM t_uniq_variadic_if WHERE cond),
    uniqIf(a, b, cond)      = (SELECT uniq(a, b)      FROM t_uniq_variadic_if WHERE cond),
    uniqHLL12If(a, b, cond) = (SELECT uniqHLL12(a, b) FROM t_uniq_variadic_if WHERE cond)
FROM t_uniq_variadic_if;

-- Nullable argument: exercises addBatchSinglePlaceNotNull with -If flags and a null map.
SELECT
    uniqExactIf(na, b, cond) = (SELECT uniqExact(na, b) FROM t_uniq_variadic_if WHERE cond),
    uniqIf(na, b, cond)      = (SELECT uniq(na, b)      FROM t_uniq_variadic_if WHERE cond),
    uniqHLL12If(na, b, cond) = (SELECT uniqHLL12(na, b) FROM t_uniq_variadic_if WHERE cond)
FROM t_uniq_variadic_if;

DROP TABLE t_uniq_variadic_if;
