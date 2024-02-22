#!/usr/bin/env -S ${HOME}/clickhouse-client --progress --queries-file


SELECT count(NULL) FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY number % 2 WITH TOTALS;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (`n` UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 SELECT * FROM numbers(10);

SET
    allow_experimental_parallel_reading_from_replicas=1,
    max_parallel_replicas=2,
    use_hedged_requests=0,
    cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree=1
;

SELECT count(NULL) FROM t1 WITH TOTALS;
SELECT count(NULL as a), a FROM t1 WITH TOTALS;

-- result differs in old and new analyzer:
-- SELECT count(NULL as a), sum(a) FROM t1 WITH TOTALS;

SELECT uniq(NULL) FROM t1 WITH TOTALS;
