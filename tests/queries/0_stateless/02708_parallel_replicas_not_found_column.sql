CREATE TABLE IF NOT EXISTS t_02708(x DateTime) ENGINE = MergeTree ORDER BY tuple();
SELECT count() FROM t_02708 SETTINGS allow_experimental_parallel_reading_from_replicas=1;
DROP TABLE t_02708;
