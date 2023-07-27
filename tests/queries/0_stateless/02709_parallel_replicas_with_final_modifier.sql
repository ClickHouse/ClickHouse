DROP TABLE IF EXISTS t_02709;
CREATE TABLE t_02709 (key UInt32, sign Int8, date Datetime) ENGINE=CollapsingMergeTree(sign) PARTITION BY date ORDER BY key;
INSERT INTO t_02709 VALUES (1, 1, '2020-01-01'), (2, 1, '2020-01-02'), (1, -1, '2020-01-01'), (2, -1, '2020-01-02'), (1, 1, '2020-01-01');
SELECT * FROM t_02709 FINAL ORDER BY key SETTINGS max_parallel_replicas=3, allow_experimental_parallel_reading_from_replicas=1, use_hedged_requests=0, cluster_for_parallel_replicas='parallel_replicas';
DROP TABLE t_02709;
