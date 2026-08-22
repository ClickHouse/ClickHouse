-- https://github.com/ClickHouse/ClickHouse/issues/58727
SELECT number % 2 AS even, aggThrow(number) FROM numbers(10) GROUP BY even; -- { serverError AGGREGATE_FUNCTION_THROW}
