-- `ReadFromMemoryStorageStep` splits PREWHERE into several steps, whose actions are `AdaptiveExpressionActions`
-- with `enable_adaptive_short_circuit_lazy_execution`. They are stateful, so every stream must have its own copy.
-- The result must match the static short-circuit schedule; a data race on a shared instance is caught by the sanitizer builds.

DROP TABLE IF EXISTS memory_prewhere;
CREATE TABLE memory_prewhere (a UInt64, b UInt64, s String) ENGINE = Memory;
INSERT INTO memory_prewhere SELECT number, number % 7, toString(number) FROM numbers(200000) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000;

SELECT count(), sum(a)
FROM memory_prewhere
PREWHERE b != 3 AND if(a % 3 = 0, intDiv(a, b + 1) % 5 != 0, length(s) > 4)
SETTINGS short_circuit_function_evaluation = 'force_enable', enable_adaptive_short_circuit_lazy_execution = 1,
    enable_multiple_prewhere_read_steps = 1, max_threads = 8, max_block_size = 1000;

SELECT count(), sum(a)
FROM memory_prewhere
PREWHERE b != 3 AND if(a % 3 = 0, intDiv(a, b + 1) % 5 != 0, length(s) > 4)
SETTINGS short_circuit_function_evaluation = 'force_enable', enable_adaptive_short_circuit_lazy_execution = 0,
    enable_multiple_prewhere_read_steps = 1, max_threads = 8, max_block_size = 1000;

DROP TABLE memory_prewhere;
