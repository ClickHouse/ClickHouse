-- This query itself is also accounted in metric.
SELECT value > 0 FROM system.metrics WHERE metric = 'Query';
