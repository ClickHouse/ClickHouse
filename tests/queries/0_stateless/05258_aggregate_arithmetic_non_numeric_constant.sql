-- An aggregate function applied to a multiplication or a division by a constant that is not a
-- number (a NULL, a tuple, an IP address) must return the same value whether or not the
-- optimization that moves such a constant out of the aggregate function is enabled.
-- The test runner randomizes that optimization, so every statement below sets it explicitly.

-- A NULL constant: multiplication, over non-empty and over empty input.
SELECT 'null_multiply_min_on', min(toNullable(number) * CAST(NULL AS Nullable(UInt64))) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'null_multiply_min_off', min(toNullable(number) * CAST(NULL AS Nullable(UInt64))) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'null_multiply_min_empty_on', min(toNullable(number) * CAST(NULL AS Nullable(UInt64))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'null_multiply_min_empty_off', min(toNullable(number) * CAST(NULL AS Nullable(UInt64))) FROM numbers(0) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- The same NULL constant on the left of the multiplication.
SELECT 'null_multiply_left_max_on', max(CAST(NULL AS Nullable(UInt64)) * toNullable(number)) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'null_multiply_left_max_off', max(CAST(NULL AS Nullable(UInt64)) * toNullable(number)) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- A NULL constant as the divisor, and with a non-nullable aggregated argument.
SELECT 'null_divide_sum_on', sum(toNullable(number) / CAST(NULL AS Nullable(UInt64))) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'null_divide_sum_off', sum(toNullable(number) / CAST(NULL AS Nullable(UInt64))) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'null_multiply_avg_on', avg(number * CAST(NULL AS Nullable(UInt64))) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'null_multiply_avg_off', avg(number * CAST(NULL AS Nullable(UInt64))) FROM numbers(5) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- A tuple constant: multiplying two tuples is a dot product, so moving the constant out of the
-- aggregate function does not preserve the order of the rows and would return a different value.
-- Both operand orders are checked: this is the only constant whose value changes if it is moved out.
SELECT 'tuple_max_on', max(v * (0.5, 0.5)) FROM (SELECT arrayJoin([(0., 10.), (1., 0.), (3., 3.)]) AS v) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'tuple_max_off', max(v * (0.5, 0.5)) FROM (SELECT arrayJoin([(0., 10.), (1., 0.), (3., 3.)]) AS v) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'tuple_min_on', min(v * (0.5, 0.5)) FROM (SELECT arrayJoin([(0., 10.), (1., 0.), (3., 3.)]) AS v) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'tuple_min_off', min(v * (0.5, 0.5)) FROM (SELECT arrayJoin([(0., 10.), (1., 0.), (3., 3.)]) AS v) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'tuple_left_max_on', max((0.5, 0.5) * v) FROM (SELECT arrayJoin([(0., 10.), (1., 0.), (3., 3.)]) AS v) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'tuple_left_max_off', max((0.5, 0.5) * v) FROM (SELECT arrayJoin([(0., 10.), (1., 0.), (3., 3.)]) AS v) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- An IP address constant.
SELECT 'ipv4_min_on', min((number + 1) * toIPv4('0.0.0.2')) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'ipv4_min_off', min((number + 1) * toIPv4('0.0.0.2')) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;

-- The same rewrite is also applied while printing the parsed query, where it must not fail either.
SELECT 'ast_null_declined', countIf(explain ILIKE '%Function max%') FROM (EXPLAIN AST optimize = 1 SELECT min(number * NULL) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1);

-- With a numeric constant the rewrite still happens: a negative factor turns min into max. These
-- four statements observe the rewrite itself, so the checks above cannot pass by it being skipped.
SELECT 'ast_hoist_on', countIf(explain ILIKE '%Function max%') FROM (EXPLAIN AST optimize = 1 SELECT min(number * -2) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1);
SELECT 'ast_hoist_off', countIf(explain ILIKE '%Function max%') FROM (EXPLAIN AST optimize = 1 SELECT min(number * -2) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0);
SELECT 'qtree_hoist_on', countIf(explain ILIKE '%function_name: max%') FROM (EXPLAIN QUERY TREE SELECT min(number * -2) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1);
SELECT 'qtree_hoist_off', countIf(explain ILIKE '%function_name: max%') FROM (EXPLAIN QUERY TREE SELECT min(number * -2) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0);

-- And it keeps returning the right values for numeric constants.
SELECT 'numeric_min_on', min(number * -2) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'numeric_min_off', min(number * -2) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT 'numeric_sum_on', sum(number * 2) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT 'numeric_sum_off', sum(number * 2) FROM numbers(3) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
