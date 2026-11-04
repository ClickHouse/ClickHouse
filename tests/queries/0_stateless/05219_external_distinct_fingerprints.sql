SET max_threads = 1, max_block_size = 2048, max_untracked_memory = 0;
SET optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0;

-- The set's byte limit measures retained fingerprints rather than the original variable-width values.
SELECT count() FROM
(
    SELECT DISTINCT [concat(toString(number), repeat('x', 4096))] AS k FROM numbers(16384)
)
SETTINGS max_bytes_in_distinct = 1048576, max_bytes_ratio_before_external_distinct = 0,
    max_bytes_before_external_distinct = 1073741824;

SET max_block_size = 2, max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 1;

-- Ordinary runs deduplicate composite keys and preserve both original columns.
SELECT k, payload FROM
(
    SELECT DISTINCT [number % 3] AS k, number % 3 AS payload FROM numbers(12)
)
ORDER BY k;

-- Internal comparison columns remain separate from user columns with the same base names. The generic key adds a
-- fingerprint, every run adds the seen count, and the order above `DISTINCT` adds an arrival number.
SELECT count() FROM
(
    SELECT DISTINCT [number % 3] AS __key_fingerprint, number % 3 AS __seen_rows, number % 3 AS __arrival_number
    FROM numbers(12)
);
SELECT groupArray(__arrival_number) FROM
(
    SELECT DISTINCT [number % 3] AS __key_fingerprint, number % 3 AS __seen_rows, number % 3 AS __arrival_number
    FROM numbers(12)
    ORDER BY __arrival_number DESC
);

-- A single input spill run is merged with an empty in-memory tail.
SELECT count() FROM (SELECT DISTINCT [number] AS k FROM numbers(2));
