-- Tags: no-replicated-database, no-darwin
-- no-darwin: VMMaxMapCount is read from /proc/sys/vm/max_map_count and VMNumMaps from /proc/self/maps — both Linux only.

SELECT least(value, 0) FROM system.asynchronous_metrics WHERE metric = 'VMMaxMapCount';
SELECT least(value, 0) FROM system.asynchronous_metrics WHERE metric = 'VMNumMaps';
