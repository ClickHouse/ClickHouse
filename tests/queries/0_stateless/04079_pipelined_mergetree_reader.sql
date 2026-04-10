-- Tags: no-random-merge-tree-settings

SET use_pipelined_mergetree_reader = 1;
SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS t_pipelined_reader;

CREATE TABLE t_pipelined_reader
(
    key UInt64,
    prewhere_col String,
    rest_col1 String,
    rest_col2 UInt64
)
ENGINE = MergeTree
ORDER BY key;

-- Insert enough rows to have multiple granules.
INSERT INTO t_pipelined_reader
SELECT
    number,
    'pw_' || toString(number % 100),
    'rest_' || toString(number),
    number * 10
FROM numbers(10000);

-- Basic PREWHERE query: should read prewhere_col in first stage, rest columns in second.
SELECT count() FROM t_pipelined_reader PREWHERE prewhere_col = 'pw_42';

-- PREWHERE with multiple rest columns.
SELECT sum(rest_col2) FROM t_pipelined_reader PREWHERE prewhere_col = 'pw_0';

-- WHERE that gets moved to PREWHERE automatically.
SELECT count(), min(rest_col2), max(rest_col2)
FROM t_pipelined_reader
WHERE key >= 5000 AND key < 5100;

-- Multiple prewhere conditions (tests multiple prewhere read steps).
SELECT count() FROM t_pipelined_reader
PREWHERE key >= 1000 AND prewhere_col = 'pw_50';

-- Verify data correctness: compare pipelined vs non-pipelined results.
SELECT 'pipelined_sum', sum(rest_col2) FROM t_pipelined_reader PREWHERE key < 100;

SET use_pipelined_mergetree_reader = 0;
SELECT 'standard_sum', sum(rest_col2) FROM t_pipelined_reader PREWHERE key < 100;

-- Test with no PREWHERE (should fall back to standard reader).
SET use_pipelined_mergetree_reader = 1;
SELECT count() FROM t_pipelined_reader WHERE rest_col2 > 99990;

-- Test with columns absent from part (defaults).
ALTER TABLE t_pipelined_reader ADD COLUMN new_col UInt64 DEFAULT 42;
SELECT sum(new_col) FROM t_pipelined_reader PREWHERE key < 10;

DROP TABLE t_pipelined_reader;
