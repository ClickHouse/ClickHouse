-- Tags: no-random-merge-tree-settings, no-object-storage
-- Disable force_primary_key_reverse_order: Tests read-in-order optimization sensitive to sort direction
SET force_primary_key_reverse_order = 0;

SET optimize_read_in_order = 1, merge_tree_min_rows_for_concurrent_read = 1000, read_in_order_use_virtual_row = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    `t` DateTime
)
ENGINE = MergeTree
ORDER BY t
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT toDateTime('2024-01-10') + number FROM numbers(10000);
INSERT INTO tab SELECT toDateTime('2024-01-30') + number FROM numbers(10000);
INSERT INTO tab SELECT toDateTime('2024-01-20') + number FROM numbers(10000);

EXPLAIN PIPELINE
SELECT *
FROM tab
ORDER BY t ASC
SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 4, read_in_order_use_buffering = 0
FORMAT tsv;