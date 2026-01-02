SET allow_experimental_analyzer = 1;

-- For function count, rewrite countState to countStateIf changes the type from AggregateFunction(count, Nullable(UInt64)) to AggregateFunction(count, UInt64)
-- We can cast AggregateFunction(count, UInt64) back to AggregateFunction(count, Nullable(UInt64)) with additional _CAST
select hex(countState(if(toNullable(number % 2 = 0), number, null))) from numbers(5) settings optimize_rewrite_aggregate_function_with_if=1;
select toTypeName(countState(if(toNullable(number % 2 = 0), number, null))) from numbers(5) settings optimize_rewrite_aggregate_function_with_if=1;
select arrayStringConcat(arraySlice(splitByString(', ', trimLeft(explain)), 2), ', ') from (explain query tree select hex(countState(if(toNullable(number % 2 = 0), number, null))) from numbers(5) settings optimize_rewrite_aggregate_function_with_if=1) where explain like '%AggregateFunction%';

-- For function uniq, rewrite uniqState to uniqStateIf changes the type from AggregateFunction(uniq, Nullable(UInt64)) to AggregateFunction(uniq, UInt64)
-- We can't cast AggregateFunction(uniq, UInt64) back to AggregateFunction(uniq, Nullable(UInt64)) so rewrite is not happening.
select toTypeName(uniqState(if(toNullable(number % 2 = 0), number, null))) from numbers(5) settings optimize_rewrite_aggregate_function_with_if=1;
select hex(uniqState(if(toNullable(number % 2 = 0), number, null))) from numbers(5) settings optimize_rewrite_aggregate_function_with_if=1;
select arrayStringConcat(arraySlice(splitByString(', ', trimLeft(explain)), 2), ', ') from (explain query tree select hex(uniqState(if(toNullable(number % 2 = 0), number, null))) from numbers(5) settings optimize_rewrite_aggregate_function_with_if=1) where explain like '%AggregateFunction%';

select '----';

CREATE TABLE a
(
    `a_id` String
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY tuple();


CREATE TABLE b
(
    `b_id` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
PARTITION BY tuple()
ORDER BY tuple();

CREATE MATERIALIZED VIEW mv TO b
(
    `b_id` AggregateFunction(uniq, Nullable(String))
)
AS SELECT uniqState(if(a_id != '', a_id, NULL)) AS b_id
FROM a;
