-- Regression test for a stack overflow that happened when LLVM JIT codegen ran deep inside
-- recursive scalar-subquery analysis. LLVM codegen is stack-hungry and does not cooperate with
-- `checkStackSize`, so on platforms with a small default thread stack (macOS secondary threads
-- get only 512 KiB) it overran the stack and crashed the server. It was only visible on
-- `arm_darwin`; here we force aggregate JIT compilation from inside scalar subqueries so the
-- crashing path is exercised. The query result itself is irrelevant - it must just not crash.

SET enable_analyzer = 1;
SET compile_aggregate_expressions = 1, min_count_to_compile_aggregate_expression = 0;
SET compile_expressions = 1, min_count_to_compile_expression = 0;

WITH
    data AS (SELECT number AS v FROM numbers(16)),
    (SELECT avg(v) FROM data) AS value1,
    (SELECT avg(v) FROM data) AS value2,
    (SELECT avg(v) FROM data) AS value3,
    (SELECT avg(v) FROM data) AS value4,
    (SELECT avg(v) FROM data) AS value5,
    (SELECT avg(v) FROM data) AS value6,
    (SELECT avg(v) FROM data) AS value7,
    (SELECT avg(v) FROM data) AS value8,
    (SELECT avg(v) FROM data) AS value9,
    (SELECT avg(v) FROM data) AS value10,
    (SELECT avg(v) FROM data) AS value11,
    (SELECT avg(v) FROM data) AS value12,
    (SELECT avg(v) FROM data) AS value13,
    (SELECT avg(v) FROM data) AS value14
SELECT value1 + value2 + value3 + value4 + value5 + value6 + value7
     + value8 + value9 + value10 + value11 + value12 + value13 + value14;
