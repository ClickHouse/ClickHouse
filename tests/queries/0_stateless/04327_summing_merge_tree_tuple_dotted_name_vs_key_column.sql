-- A Tuple element whose own name contains a dot (for example `p.x`) is flattened to a name
-- like `metrics.p.x`. This must not be confused with a separate physical column `metrics.p`
-- that happens to be in the partition or sorting key. The tuple element should still be
-- aggregated; only the real key column is excluded from aggregation.

DROP TABLE IF EXISTS t_dotted_partition;
DROP TABLE IF EXISTS t_dotted_sorting;
DROP TABLE IF EXISTS t_dotted_sorting_coalescing;
DROP TABLE IF EXISTS t_dotted_partition_coalescing;

SELECT 'Summing, dotted element vs partition-key column';
CREATE TABLE t_dotted_partition
(
    id UInt64,
    metrics Tuple(`p.x` UInt64, y UInt64),
    `metrics.p` UInt64
)
ENGINE = SummingMergeTree
PARTITION BY `metrics.p`
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_dotted_partition VALUES (1, (10, 100), 5), (1, (20, 200), 5);
SELECT id, metrics, `metrics.p` FROM t_dotted_partition FINAL ORDER BY id;

SELECT 'Summing, dotted element vs sorting-key column';
CREATE TABLE t_dotted_sorting
(
    metrics Tuple(`p.x` UInt64, y UInt64),
    `metrics.p` UInt64
)
ENGINE = SummingMergeTree
ORDER BY `metrics.p`
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_dotted_sorting VALUES ((10, 100), 5), ((20, 200), 5);
SELECT metrics, `metrics.p` FROM t_dotted_sorting FINAL ORDER BY `metrics.p`;

SELECT 'Coalescing, dotted element vs sorting-key column';
CREATE TABLE t_dotted_sorting_coalescing
(
    metrics Tuple(`p.x` Nullable(UInt64), y Nullable(UInt64)),
    `metrics.p` UInt64
)
ENGINE = CoalescingMergeTree
ORDER BY `metrics.p`
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_dotted_sorting_coalescing VALUES ((NULL, 100), 5), ((20, 200), 5);
SELECT metrics, `metrics.p` FROM t_dotted_sorting_coalescing FINAL ORDER BY `metrics.p`;

SELECT 'Coalescing, dotted element vs partition-key column';
CREATE TABLE t_dotted_partition_coalescing
(
    id UInt64,
    metrics Tuple(`p.x` Nullable(UInt64), y Nullable(UInt64)),
    `metrics.p` UInt64
)
ENGINE = CoalescingMergeTree
PARTITION BY `metrics.p`
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO t_dotted_partition_coalescing VALUES (1, (NULL, 100), 5), (1, (20, 200), 5);
SELECT id, metrics, `metrics.p` FROM t_dotted_partition_coalescing FINAL ORDER BY id;

DROP TABLE t_dotted_partition;
DROP TABLE t_dotted_sorting;
DROP TABLE t_dotted_sorting_coalescing;
DROP TABLE t_dotted_partition_coalescing;
