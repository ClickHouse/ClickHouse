-- `DiskTotal` is a key-value metric: a single row with a Map from the disk name to the total size,
-- and NaN in the `value` column.
SELECT isNaN(value), key_values['default'] > 0 FROM system.asynchronous_metrics WHERE metric = 'DiskTotal';

-- There are no flattened per-disk metrics (like `DiskTotal_default`) anymore.
SELECT count() FROM system.asynchronous_metrics WHERE metric LIKE 'DiskTotal\_%';

-- Scalar metrics have an empty map in the `key_values` column.
SELECT empty(key_values), NOT isNaN(value) FROM system.asynchronous_metrics WHERE metric = 'AsynchronousMetricsUpdateInterval';

-- In the log, a key-value metric is written as one row per key.
SYSTEM FLUSH LOGS asynchronous_metric_log;
SELECT count() > 0 FROM system.asynchronous_metric_log WHERE metric = 'DiskTotal' AND key = 'default';
SELECT count() FROM system.asynchronous_metric_log WHERE metric LIKE 'DiskTotal\_%';
