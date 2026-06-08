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

-- The update is a value-changing expression: if a partial mutation result were ever
-- swapped into the table, some rows would have `a = b + 1` and the `sum(a = b)`
-- check below would observe fewer than 20 matches. A no-op update (`a = a + 0`)
-- would not detect such a regression.
ALTER TABLE memory_mutation_timeout_break
    UPDATE a = a + 1 + toUInt64(sleepEachRow(0.01)) WHERE b >= 0
SETTINGS
    function_sleep_max_microseconds_per_block = 10000000,
    max_block_size = 1,
    max_execution_time = 0.001,
    timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

SELECT count(), sum(a = b)
FROM memory_mutation_timeout_break;

DROP TABLE memory_mutation_timeout_break;
