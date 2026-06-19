-- Test that PREWHERE conditions on multiple subcolumns of the same Map column
-- are grouped into a single read step, avoiding redundant deserialization.

SET enable_multiple_prewhere_read_steps = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS test_map_prewhere;

CREATE TABLE test_map_prewhere
(
    id UInt64,
    tags Map(String, String),
    value UInt32
) ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO test_map_prewhere
SELECT
    number,
    map('k0', toString(number % 10), 'k1', toString(number % 20), 'k2', toString(number % 30)),
    number
FROM numbers(100000);

-- All Map subcolumn conditions should be grouped into one PREWHERE step
-- because they all read from the same physical column.
-- With the fix we expect 1 Prewhere filter line (single step),
-- without the fix there would be 3 separate Prewhere filter lines.
SELECT count() FROM (
    EXPLAIN actions=1
    SELECT * FROM test_map_prewhere
    PREWHERE tags['k0'] != '' AND tags['k1'] != '' AND tags['k2'] != ''
) WHERE explain LIKE '%Prewhere filter%';

-- Verify correctness of the query result.
SELECT count() FROM test_map_prewhere
PREWHERE tags['k0'] != '' AND tags['k1'] != '' AND tags['k2'] != '';

-- Mixed case: Map subcolumn + different physical column should still split into 2 steps.
SELECT count() FROM (
    EXPLAIN actions=1
    SELECT * FROM test_map_prewhere
    PREWHERE tags['k0'] != '' AND value > 50000
) WHERE explain LIKE '%Prewhere filter%';

DROP TABLE test_map_prewhere;
