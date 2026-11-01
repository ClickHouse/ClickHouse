-- Tuple comparison with group_by_use_nulls and ROLLUP/CUBE.
-- The ComparisonTupleEliminationPass must propagate the Nullable wrapper
-- so that the optimized expression name matches the aggregation key.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=96419&sha=b7ee113a859d63ecd96f17cab2ab7f79ae9c0145&name_0=PR&name_1=AST%20fuzzer%20%28amd_ubsan%29

SELECT tuple(2) = tuple(materialize(1)) GROUP BY 1 WITH ROLLUP SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;
SELECT tuple(2) = tuple(materialize(1)) GROUP BY 1 WITH ROLLUP WITH TOTALS SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;
SELECT tuple(2) = tuple(materialize(1)) GROUP BY 1 WITH CUBE SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;
SELECT tuple(2) != tuple(materialize(1)) GROUP BY 1 WITH ROLLUP SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;
SELECT (1, 2) = (materialize(1), materialize(2)) GROUP BY 1 WITH ROLLUP SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;
SELECT tuple(toNullable(2)) = tuple(materialize(1)) GROUP BY 1 WITH ROLLUP SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;
SELECT (2,) = tuple(materialize(1)) GROUP BY 1 WITH ROLLUP SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;
