-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- PREWHERE steps and row-level filters of `MergeTree` and `Memory` tables are compiled by the JIT,
-- and give the same results as interpreted.

DROP TABLE IF EXISTS jit_prewhere_mt;
DROP TABLE IF EXISTS jit_prewhere_memory;

CREATE TABLE jit_prewhere_mt (k UInt64, value UInt32, x Float64, n Nullable(Int64), s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE jit_prewhere_memory (k UInt64, value UInt32, x Float64, n Nullable(Int64), s String) ENGINE = Memory;

INSERT INTO jit_prewhere_mt SELECT number, number % 1000, number % 977 / 7, if(number % 5 = 0, NULL, number % 301 - 150), toString(number % 13) FROM numbers(100000);
INSERT INTO jit_prewhere_memory SELECT * FROM jit_prewhere_mt;

SET log_queries = 1;

-- Each query runs with the JIT on and off; both lines of a pair must be equal.

SELECT 'MergeTree, arithmetic on one column';
SELECT count(), sum(k) FROM jit_prewhere_mt PREWHERE bitAnd(value * 2654435761, 1023) < 500 AND value * 3 + 7 > 100
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'jit_prewhere_mt_compiled';
SELECT count(), sum(k) FROM jit_prewhere_mt PREWHERE bitAnd(value * 2654435761, 1023) < 500 AND value * 3 + 7 > 100
SETTINGS compile_expressions = 0;

SELECT 'MergeTree, conditions on different columns';
SELECT count(), sum(k) FROM jit_prewhere_mt PREWHERE value * 2 + 1 > 500 AND x * 3 - 1 < 200 AND s = '7'
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT count(), sum(k) FROM jit_prewhere_mt PREWHERE value * 2 + 1 > 500 AND x * 3 - 1 < 200 AND s = '7'
SETTINGS compile_expressions = 0;

SELECT 'MergeTree, guarded division does not throw';
SELECT count(), sum(k) FROM jit_prewhere_mt PREWHERE value + 0 != 0 AND intDiv(1000, value) > 3 AND value * 2 < 1500
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT count(), sum(k) FROM jit_prewhere_mt PREWHERE value + 0 != 0 AND intDiv(1000, value) > 3 AND value * 2 < 1500
SETTINGS compile_expressions = 0;

SELECT 'MergeTree, Nullable';
SELECT count(), sum(k) FROM jit_prewhere_mt PREWHERE n * 2 + 1 > 10 AND n - 3 < 100
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT count(), sum(k) FROM jit_prewhere_mt PREWHERE n * 2 + 1 > 10 AND n - 3 < 100
SETTINGS compile_expressions = 0;

SELECT 'MergeTree, WHERE moved to PREWHERE';
SELECT count(), sum(k) FROM jit_prewhere_mt WHERE bitAnd(value * 2654435761, 1023) < 500 AND value * 3 + 7 > 100
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT count(), sum(k) FROM jit_prewhere_mt WHERE bitAnd(value * 2654435761, 1023) < 500 AND value * 3 + 7 > 100
SETTINGS compile_expressions = 0;

SELECT 'Memory, arithmetic on one column';
SELECT count(), sum(k) FROM jit_prewhere_memory PREWHERE bitAnd(value * 2654435761, 1023) < 500 AND value * 3 + 7 > 100
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'jit_prewhere_memory_compiled';
SELECT count(), sum(k) FROM jit_prewhere_memory PREWHERE bitAnd(value * 2654435761, 1023) < 500 AND value * 3 + 7 > 100
SETTINGS compile_expressions = 0;

SELECT 'Memory, guarded division does not throw';
SELECT count(), sum(k) FROM jit_prewhere_memory PREWHERE value + 0 != 0 AND intDiv(1000, value) > 3 AND value * 2 < 1500
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT count(), sum(k) FROM jit_prewhere_memory PREWHERE value + 0 != 0 AND intDiv(1000, value) > 3 AND value * 2 < 1500
SETTINGS compile_expressions = 0;

-- A row-level filter is built by the same readers.
DROP ROW POLICY IF EXISTS jit_prewhere_policy_mt ON jit_prewhere_mt;
DROP ROW POLICY IF EXISTS jit_prewhere_policy_memory ON jit_prewhere_memory;
CREATE ROW POLICY jit_prewhere_policy_mt ON jit_prewhere_mt USING bitAnd(value * 2654435761, 1023) < 700 AND value * 3 + 7 > 50 TO ALL;
CREATE ROW POLICY jit_prewhere_policy_memory ON jit_prewhere_memory USING bitAnd(value * 2654435761, 1023) < 700 AND value * 3 + 7 > 50 TO ALL;

SELECT 'MergeTree, row-level filter';
SELECT count(), sum(k) FROM jit_prewhere_mt
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'jit_prewhere_mt_row_policy_compiled';
SELECT count(), sum(k) FROM jit_prewhere_mt
SETTINGS compile_expressions = 0;

SELECT 'Memory, row-level filter';
SELECT count(), sum(k) FROM jit_prewhere_memory
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT count(), sum(k) FROM jit_prewhere_memory
SETTINGS compile_expressions = 0;

DROP ROW POLICY jit_prewhere_policy_mt ON jit_prewhere_mt;
DROP ROW POLICY jit_prewhere_policy_memory ON jit_prewhere_memory;

SYSTEM FLUSH LOGS query_log;

-- The filters ran compiled.
SELECT log_comment, ProfileEvents['CompiledFunctionRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'jit_prewhere_%_compiled' AND event_date >= yesterday()
ORDER BY log_comment;

DROP TABLE jit_prewhere_mt;
DROP TABLE jit_prewhere_memory;
