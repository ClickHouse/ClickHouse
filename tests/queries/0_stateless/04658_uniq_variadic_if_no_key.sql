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

-- Nullable uniq argument, plain -If: routes through AggregateFunctionIfNullVariadic, which merges
-- cond + the na null-mask into final_null_flags and calls the nested uniq via
-- addBatchSinglePlaceNotNull(..., -1). This exercises the NotNull batch method with a null map but
-- with if_argument_pos == -1 (the flags == nullptr path), not the -If flags branch.
SELECT
    uniqExactIf(na, b, cond) = (SELECT uniqExact(na, b) FROM t_uniq_variadic_if WHERE cond),
    uniqIf(na, b, cond)      = (SELECT uniq(na, b)      FROM t_uniq_variadic_if WHERE cond),
    uniqHLL12If(na, b, cond) = (SELECT uniqHLL12(na, b) FROM t_uniq_variadic_if WHERE cond)
FROM t_uniq_variadic_if;

-- Nullable uniq argument, -If nested under -OrDefault: this is the only path that reaches
-- AggregateFunctionUniqVariadic::addBatchSinglePlaceNotNull with if_argument_pos >= 0 (the -If
-- flags branch). Because -If is no longer the outermost combinator its own null adapter is not
-- used; the generic Null adapter wraps the whole function and calls addBatchSinglePlaceNotNull,
-- OrFill forwards it, and AggregateFunctionIf::addBatchSinglePlaceNotNull hands the nested uniq the
-- filter column position (num_arguments - 1) while the null map carries na's nulls.
SELECT
    uniqExactIfOrDefault(na, b, cond) = (SELECT uniqExact(na, b) FROM t_uniq_variadic_if WHERE cond),
    uniqIfOrDefault(na, b, cond)      = (SELECT uniq(na, b)      FROM t_uniq_variadic_if WHERE cond),
    uniqHLL12IfOrDefault(na, b, cond) = (SELECT uniqHLL12(na, b) FROM t_uniq_variadic_if WHERE cond)
FROM t_uniq_variadic_if;

DROP TABLE t_uniq_variadic_if;
