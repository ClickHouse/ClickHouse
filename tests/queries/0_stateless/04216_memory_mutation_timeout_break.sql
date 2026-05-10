DROP TABLE IF EXISTS memory_mutation_timeout_break;

CREATE TABLE memory_mutation_timeout_break
(
    a UInt64,
    b UInt64
)
ENGINE = Memory;

INSERT INTO memory_mutation_timeout_break
SELECT
    number,
    number
FROM numbers(20)
SETTINGS max_block_size = 1;

ALTER TABLE memory_mutation_timeout_break
    UPDATE a = a + toUInt64(sleepEachRow(0.01)) WHERE b >= 0
SETTINGS
    function_sleep_max_microseconds_per_block = 10000000,
    max_block_size = 1,
    max_execution_time = 0.001,
    timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

SELECT count(), sum(a = b)
FROM memory_mutation_timeout_break;

DROP TABLE memory_mutation_timeout_break;
