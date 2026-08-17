-- Use UInt128 to exercise the generic ColumnVector filtering path on every supported CPU.
-- The predicate creates three selected runs inside each 64-row filter block.
SET max_block_size = 256;

SELECT count(), sum(value)
FROM
(
    SELECT toUInt128(number) AS value
    FROM numbers(256)
)
WHERE (value % 64 BETWEEN 4 AND 11)
   OR (value % 64 BETWEEN 20 AND 35)
   OR (value % 64 BETWEEN 48 AND 55);
