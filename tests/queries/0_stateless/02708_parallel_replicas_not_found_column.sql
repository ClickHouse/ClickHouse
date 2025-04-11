CREATE TABLE IF NOT EXISTS t_02708(x DateTime) ENGINE = MergeTree ORDER BY tuple();
SET send_logs_level='error';
SELECT count() FROM t_02708 SETTINGS enable_parallel_replicas=1;
DROP TABLE t_02708;
