-- Tags: no-fasttest
-- no-fasttest: needs the server-side AST fuzzer (ast_fuzzer_runs)

-- Regression test for #109706: the AST fuzzer wrapped a data-type argument in an expression
-- function (e.g. multiply()/if()/multiIf()), producing an ASTDataType whose argument is an
-- ASTFunction. That node cannot be produced by ParserDataType, so ASTDataType::formatImpl
-- emitted text that parses back to a different AST, tripping the format-parse-format
-- consistency check in executeQuery (LOGICAL_ERROR "Inconsistent AST formatting", which aborts
-- on DEBUG/sanitizer builds). The fuzzer must not inject expressions into data-type argument
-- lists. This runs the fuzzer over CREATE queries with argument-bearing types; before the fix
-- it aborted the server, now it just runs the fuzzed queries harmlessly.

SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 20;
SET ast_fuzzer_any_query = 1;

CREATE TABLE t_04344 (a Nullable(Int32), b LowCardinality(String), c Array(Nullable(UInt64)), d Map(String, Int64), e FixedString(8), f JSON(max_dynamic_paths=8, p1 UInt32, p2 Array(String), SKIP s), g QBit(Float32, 16), h SimpleAggregateFunction(sumMap, Tuple(Array(String), Array(UInt64))), n SimpleAggregateFunction(sum, UInt64), i DateTime('Asia/Istanbul'), j DateTime64(3, 'UTC'), k Nested(x UInt32, y Array(Nullable(Int64))), l AggregateFunction(quantileExact(0.5), Float64), m AggregateFunction(topK(10), String)) ENGINE = Memory;

SELECT 1;
