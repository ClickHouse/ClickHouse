-- Checks that the `TotalUncompressedBytesOfMergeTreeTables` and
-- `TotalUncompressedBytesOfMergeTreeTablesSystem` asynchronous metrics are emitted,
-- and that the global accumulator uses the same source as the `total_bytes_uncompressed`
-- column of `system.tables` (so it accounts for at least this table's uncompressed bytes).

DROP TABLE IF EXISTS t_total_uncompressed_metric SYNC;

CREATE TABLE t_total_uncompressed_metric (a UInt64, b String)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_total_uncompressed_metric
SELECT number, toString(number) FROM numbers(100000);

SYSTEM RELOAD ASYNCHRONOUS METRICS;

-- Both metrics must be present (this would catch a typo in the metric name).
SELECT count() FROM system.asynchronous_metrics
WHERE metric IN ('TotalUncompressedBytesOfMergeTreeTables', 'TotalUncompressedBytesOfMergeTreeTablesSystem');

-- The global metric must account for at least this table's uncompressed bytes.
-- The metric sums over all tables, so this invariant is stable under parallel execution.
WITH
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'TotalUncompressedBytesOfMergeTreeTables') AS total_metric,
    (SELECT total_bytes_uncompressed FROM system.tables WHERE database = currentDatabase() AND name = 't_total_uncompressed_metric') AS table_bytes
SELECT table_bytes > 0 AND total_metric >= table_bytes;

DROP TABLE t_total_uncompressed_metric SYNC;
