-- A large disjunctive (OR/AND) boolean filter is JIT-compiled and evaluated by parallel
-- FilterTransform threads. On a native macOS/aarch64 build whose LLVM host triple wrongly
-- reported Linux, the AArch64 backend did not reserve x18, so the compiled expression used
-- the OS-reserved x18 register and the server crashed with SIGSEGV. This checks the query is
-- compiled, returns the correct result, and the server stays up.

DROP TABLE IF EXISTS t_jit_disjunctive_filter;

CREATE TABLE t_jit_disjunctive_filter (a Int32, b Int32, c Int32, d Int32, e Int32, f Int32) ENGINE = Memory;
INSERT INTO t_jit_disjunctive_filter
    SELECT number % 7, number % 11, number % 13, number % 17, number % 19, number % 23 FROM numbers(1000000);

SELECT count() FROM t_jit_disjunctive_filter WHERE
    (a = b AND (c = d OR (e = f AND a = c)))
    AND ((c = a AND a = d) OR (e = b AND f = c))
    AND (d = e OR (b = c AND a = f))
    AND (f = a AND (e = d OR b = c))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, max_threads = 16;

DROP TABLE t_jit_disjunctive_filter;
