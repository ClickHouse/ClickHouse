-- Tags: no-replicated-database

SELECT least(value, 0) FROM system.asynchronous_metrics WHERE metric = 'VMMaxMapCount';
SELECT least(value, 0) FROM system.asynchronous_metrics WHERE metric = 'VMNumMaps';
