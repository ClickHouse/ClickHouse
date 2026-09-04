-- Regression: with optimize_final_sequential_partitions, partition order is derived
-- from minmax index values. These must be compared with accurateLess (the same
-- semantics as the minmax index and sort key), not Field::operator<. A partition
-- whose ordering column is all-NULL stores NULL as the POSITIVE_INFINITY sentinel
-- (NULLS LAST). With Field::operator< the NULL partition could be ordered before a
-- numeric partition, and ConcatProcessor + LIMIT would then return rows from the
-- NULL partition ahead of non-NULL rows, contradicting the NULLS LAST order.

DROP TABLE IF EXISTS t_seq_null;

CREATE TABLE t_seq_null
(
    p Nullable(Int64),
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY p
ORDER BY p
SETTINGS allow_nullable_key = 1;

SYSTEM STOP MERGES t_seq_null;

-- Three partitions (NULL, 1, 2), two parts each so FINAL has something to merge.
INSERT INTO t_seq_null SELECT NULL, sumState(toUInt64(1)) FROM numbers(3) GROUP BY number % 1;
INSERT INTO t_seq_null SELECT NULL, sumState(toUInt64(2)) FROM numbers(3) GROUP BY number % 1;
INSERT INTO t_seq_null SELECT 1, sumState(toUInt64(1)) FROM numbers(3) GROUP BY number % 1;
INSERT INTO t_seq_null SELECT 1, sumState(toUInt64(2)) FROM numbers(3) GROUP BY number % 1;
INSERT INTO t_seq_null SELECT 2, sumState(toUInt64(1)) FROM numbers(3) GROUP BY number % 1;
INSERT INTO t_seq_null SELECT 2, sumState(toUInt64(2)) FROM numbers(3) GROUP BY number % 1;

-- Baseline: plain FINAL, NULLS LAST means top-2 ascending is 1, 2.
SELECT 'baseline';
SELECT p FROM t_seq_null FINAL
ORDER BY p ASC LIMIT 2
SETTINGS optimize_final_limit_pushdown = 0, optimize_final_sequential_partitions = 0;

-- Sequential partitions: must produce the same result. With Field::operator< the
-- all-NULL partition would be ordered first and leak its rows into the top-2.
SELECT 'sequential_partitions';
SELECT p FROM t_seq_null FINAL
ORDER BY p ASC LIMIT 2
SETTINGS optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1, do_not_merge_across_partitions_select_final = 1;

DROP TABLE t_seq_null;
