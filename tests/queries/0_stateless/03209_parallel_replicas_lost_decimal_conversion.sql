DROP TABLE IF EXISTS t_03209 SYNC;

CREATE TABLE t_03209 ( `a` Decimal(18, 0), `b` Decimal(18, 1), `c` Decimal(36, 0) ) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_03209', 'r1') ORDER BY tuple();
INSERT INTO t_03209 VALUES ('33', '44.4', '35');

SET max_parallel_replicas = 2, cluster_for_parallel_replicas='parallel_replicas';

SELECT * FROM t_03209 WHERE a IN toDecimal32('33.3000', 4) SETTINGS allow_experimental_parallel_reading_from_replicas=0;
SELECT * FROM t_03209 WHERE a IN toDecimal32('33.3000', 4) SETTINGS allow_experimental_parallel_reading_from_replicas=1;

DROP TABLE t_03209 SYNC;
