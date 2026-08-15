-- `optimize_if_chain_to_multiif` must keep rewriting constant-string `if`-chains to `multiIf` when
-- `optimize_if_transform_const_strings_to_lowcardinality` is enabled. Previously the synthesized
-- `multiIf` was built with the LowCardinality optimization and resolved to `LowCardinality(String)`,
-- while the outer `if` of the chain resolved to plain `String` (`FunctionIf` strips LowCardinality
-- from its arguments before inferring the return type). The type-equality guard then rejected the
-- rewrite, so enabling the setting silently disabled `optimize_if_chain_to_multiif`.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/25272

SET enable_analyzer = 1;
SET optimize_if_chain_to_multiif = 1;
SET optimize_if_transform_strings_to_enum = 0;

SELECT 'multiIf present, optimize_if_transform_const_strings_to_lowcardinality = 1';
SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SELECT countIf(explain LIKE '%multiIf%') > 0
FROM (EXPLAIN QUERY TREE SELECT if(number = 0, 'a', if(number = 1, 'b', 'c')) FROM numbers(1));

SELECT 'multiIf present, optimize_if_transform_const_strings_to_lowcardinality = 0';
SET optimize_if_transform_const_strings_to_lowcardinality = 0;
SELECT countIf(explain LIKE '%multiIf%') > 0
FROM (EXPLAIN QUERY TREE SELECT if(number = 0, 'a', if(number = 1, 'b', 'c')) FROM numbers(1));

SELECT 'result type and values, optimize_if_transform_const_strings_to_lowcardinality = 1';
SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SELECT number, if(number = 0, 'a', if(number = 1, 'b', 'c')) AS x, toTypeName(x) FROM numbers(3) ORDER BY number;
SELECT number, if(number = 0, 'a', if(number = 1, NULL, 'c')) AS x, toTypeName(x) FROM numbers(3) ORDER BY number;

-- The optimization requires all the result branches to be constants, so a nested `if` chain keeps plain
-- `String`: the else-branch of the outer `if` is another `if`, not a constant (and `FunctionIf` strips
-- `LowCardinality` from its arguments before inferring the return type). The equivalent explicit `multiIf`
-- does see all the constant leaves and returns `LowCardinality(String)`. This asymmetry is what the
-- description of the setting documents; pin it here for both syntaxes, with and without the chain rewrite.
SELECT 'nested `if` chain vs explicit `multiIf`, optimize_if_chain_to_multiif = 1';
SET optimize_if_chain_to_multiif = 1;
SELECT toTypeName(if(number = 0, 'a', if(number = 1, 'b', 'c'))) FROM numbers(1);
SELECT toTypeName(multiIf(number = 0, 'a', number = 1, 'b', 'c')) FROM numbers(1);

SELECT 'nested `if` chain vs explicit `multiIf`, optimize_if_chain_to_multiif = 0';
SET optimize_if_chain_to_multiif = 0;
SELECT toTypeName(if(number = 0, 'a', if(number = 1, 'b', 'c'))) FROM numbers(1);
SELECT toTypeName(multiIf(number = 0, 'a', number = 1, 'b', 'c')) FROM numbers(1);
