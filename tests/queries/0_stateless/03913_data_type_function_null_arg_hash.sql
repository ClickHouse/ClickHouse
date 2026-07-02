-- Regression test: `DataTypeFunction::updateHashImpl` must handle null argument types
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=b9e68f4b9b0b33c7db43b00afb3eff4ff2050694&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_ubsan%29
-- Pinned to the old analyzer. New-analyzer coverage lives in `04307_arrayfold_unresolved_lambda_init.sql`.
SET enable_analyzer = 0;
SELECT arrayFold((acc, x) -> plus(acc, toString(NULL, toLowCardinality(toUInt128(4)), materialize(4), 'aaaa', materialize(4), 4, 4, 1), x), range(number), ((acc, x) -> if(x % 2, arrayPushFront(acc, x), arrayPushBack(acc, x)))) FROM system.numbers LIMIT 0; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
