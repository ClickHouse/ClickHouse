-- `multiIf` casts its branches to the result type internally, and lifting `Decimal32(0)` to `Decimal32(3)`
-- overflows for the rows masked by the outer `if`. The adaptive heuristic must not switch such a branch
-- to eager execution, even after many blocks of profiling.

SET short_circuit_function_evaluation = 'force_enable', max_threads = 1, max_block_size = 8192, compile_expressions = 0, optimize_multiif_to_if = 0;

DROP TABLE IF EXISTS t_masked_decimal;
CREATE TABLE t_masked_decimal (ok UInt8, c1 UInt8, c2 UInt8, d Decimal32(0), e Decimal32(3)) ENGINE = Memory;

INSERT INTO t_masked_decimal
SELECT number % 101 != 0, number % 2 = 0, number % 3 = 0, if(number % 101 = 0, 999999999, 1), toDecimal32(0, 3)
FROM numbers(300000)
SETTINGS max_block_size = 8192, min_insert_block_size_rows = 8192, min_insert_block_size_bytes = 0;

SELECT sum(if(ok, multiIf(c1, d, c2, d, e), e)) FROM t_masked_decimal SETTINGS enable_adaptive_short_circuit_lazy_execution = 0;
SELECT sum(if(ok, multiIf(c1, d, c2, d, e), e)) FROM t_masked_decimal SETTINGS enable_adaptive_short_circuit_lazy_execution = 1;

DROP TABLE t_masked_decimal;
