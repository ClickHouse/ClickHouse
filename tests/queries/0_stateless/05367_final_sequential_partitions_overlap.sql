-- `optimize_final_sequential_partitions` reads partitions one after another through `Concat`
-- only when the partitions do not overlap on the partition-related sorting key column.
-- When they do overlap, it must fall back to the regular parallel read, otherwise the
-- top-N would be taken from the first partition only.

DROP TABLE IF EXISTS t_seq_part_overlap;

-- Partitions by the parity of `key`: both partitions span the whole range of `key`.
CREATE TABLE t_seq_part_overlap (key UInt64, val SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree
PARTITION BY key % 2
ORDER BY key;

-- Two parts per partition so that FINAL has something to merge in each of them.
SYSTEM STOP MERGES t_seq_part_overlap;
INSERT INTO t_seq_part_overlap SELECT number, 1 FROM numbers(100);
INSERT INTO t_seq_part_overlap SELECT number, 2 FROM numbers(100);

SELECT 'overlap_top5';
SELECT key, val
FROM t_seq_part_overlap FINAL
ORDER BY key
LIMIT 5
SETTINGS optimize_read_in_order = 1,
         optimize_final_limit_pushdown = 1,
         optimize_final_sequential_partitions = 1,
         do_not_merge_across_partitions_select_final = 1;

SELECT 'overlap_top5_desc';
SELECT key, val
FROM t_seq_part_overlap FINAL
ORDER BY key DESC
LIMIT 5
SETTINGS optimize_read_in_order = 1,
         optimize_final_limit_pushdown = 1,
         optimize_final_sequential_partitions = 1,
         do_not_merge_across_partitions_select_final = 1;

SELECT 'overlap_no_concat';
SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT key, val
    FROM t_seq_part_overlap FINAL
    ORDER BY key
    LIMIT 5
    SETTINGS optimize_read_in_order = 1,
             optimize_final_limit_pushdown = 1,
             optimize_final_sequential_partitions = 1,
             do_not_merge_across_partitions_select_final = 1
)
WHERE explain LIKE '%Concat%';

-- Sanity check: with non-overlapping partitions the same query reads them through `Concat`.
DROP TABLE t_seq_part_overlap;

CREATE TABLE t_seq_part_overlap (key UInt64, val SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree
PARTITION BY intDiv(key, 50)
ORDER BY key;

SYSTEM STOP MERGES t_seq_part_overlap;
INSERT INTO t_seq_part_overlap SELECT number, 1 FROM numbers(100);
INSERT INTO t_seq_part_overlap SELECT number, 2 FROM numbers(100);

SELECT 'no_overlap_has_concat';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, val
    FROM t_seq_part_overlap FINAL
    ORDER BY key
    LIMIT 5
    SETTINGS optimize_read_in_order = 1,
             optimize_final_limit_pushdown = 1,
             optimize_final_sequential_partitions = 1,
             do_not_merge_across_partitions_select_final = 1
)
WHERE explain LIKE '%Concat%';

DROP TABLE t_seq_part_overlap;
