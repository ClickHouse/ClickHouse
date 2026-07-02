-- Test: exercises `JSONExtractKeysAndValues` with non-constant LowCardinality(String) JSON input column
-- Covers: src/Functions/FunctionsJSON.cpp:537-557 — the `functionForcesTheReturnType<Impl>()` branch
-- in `ExecutableFunctionJSON::executeImpl` that calls `recursiveRemoveLowCardinality` on input
-- columns. This branch is currently exercised only by JSONExtract (00918_json_functions.sql:329-334),
-- never by JSONExtractKeysAndValues despite using the same code path.
SELECT JSONExtractKeysAndValues(materialize(toLowCardinality('{"a": "hello", "b": "world"}')), 'String');
SELECT JSONExtractKeysAndValues(materialize(toLowCardinality('{"a": "hello", "b": "world"}')), 'LowCardinality(String)');
SELECT JSONExtractKeysAndValues(materialize(toLowCardinality('{"a": 1, "b": 2}')), 'Int32');
SELECT JSONExtractKeysAndValues(materialize(toLowCardinality('{"a": 1, "b": 2}')), 'LowCardinality(Int32)');
-- Multi-row LC input column to exercise per-row path.
SELECT JSONExtractKeysAndValues(materialize(toLowCardinality('{"a": "x", "b": "y"}')), 'String') FROM numbers(2);
