DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;

CREATE TABLE t1(k UInt32, v String) ENGINE ReplicatedMergeTree('/02946_parallel_replicas/{database}/test_tbl', 'r1') ORDER BY k;
CREATE TABLE t2(k UInt32, v String) ENGINE ReplicatedMergeTree('/02946_parallel_replicas/{database}/test_tbl', 'r2') ORDER BY k;
CREATE TABLE t3(k UInt32, v String) ENGINE ReplicatedMergeTree('/02946_parallel_replicas/{database}/test_tbl', 'r3') ORDER BY k;

insert into t1 select number % 4, toString(number) from numbers(1000, 1000);
insert into t2 select number % 4, toString(number) from numbers(2000, 1000);
insert into t3 select number % 4, toString(number) from numbers(3000, 1000);

system sync replica t1;
system sync replica t2;
system sync replica t3;

-- w/o parallel replicas
SELECT
    k,
    count()
FROM t1
WHERE k > 0
GROUP BY k
ORDER BY k
SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;

-- parallel replicas, primary key is used
SET enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
SELECT
    k,
    count()
FROM t1
WHERE k > 0
GROUP BY k
ORDER BY k
SETTINGS force_primary_key = 1;

-- parallel replicas, primary key is NOT used
SELECT
    k,
    count()
FROM t1
GROUP BY k
ORDER BY k
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;
