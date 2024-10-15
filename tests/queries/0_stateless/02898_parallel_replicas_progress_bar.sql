DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;

CREATE TABLE t1(k UInt32, v String) ENGINE ReplicatedMergeTree('/02898_parallel_replicas/{database}/test_tbl', 'r1') ORDER BY k;
CREATE TABLE t2(k UInt32, v String) ENGINE ReplicatedMergeTree('/02898_parallel_replicas/{database}/test_tbl', 'r2') ORDER BY k;
CREATE TABLE t3(k UInt32, v String) ENGINE ReplicatedMergeTree('/02898_parallel_replicas/{database}/test_tbl', 'r3') ORDER BY k;

insert into t1 select number, toString(number) from numbers(1000, 1000);
insert into t2 select number, toString(number) from numbers(2000, 1000);
insert into t3 select number, toString(number) from numbers(3000, 1000);

system sync replica t1;
system sync replica t2;
system sync replica t3;

SET enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

-- default coordinator
SELECT count(), min(k), max(k), avg(k) FROM t1 SETTINGS log_comment='02898_default_190aed82-2423-413b-ad4c-24dcca50f65b';

-- check logs
SYSTEM FLUSH LOGS;
SET max_rows_to_read = 0; -- system.text_log can be really big
SELECT count() > 0 FROM system.text_log
WHERE query_id in (select query_id from system.query_log where current_database = currentDatabase() AND log_comment='02898_default_190aed82-2423-413b-ad4c-24dcca50f65b')
    AND message LIKE '%Total rows to read: 3000%' SETTINGS enable_parallel_replicas=0;

-- reading in order coordinator
-- disable parallel_replicas_local_plan since the test relay on traces which only present in case of no local plan
SELECT k, sipHash64(v) FROM t1 order by k limit 5 offset 998 SETTINGS optimize_read_in_order=1, parallel_replicas_local_plan=0, log_comment='02898_inorder_190aed82-2423-413b-ad4c-24dcca50f65b';

SYSTEM FLUSH LOGS;
SELECT count() > 0 FROM system.text_log
WHERE query_id in (select query_id from system.query_log where current_database = currentDatabase() AND log_comment='02898_inorder_190aed82-2423-413b-ad4c-24dcca50f65b')
    AND message LIKE '%Updated total rows to read: added % rows, total 3000 rows%' SETTINGS enable_parallel_replicas=0;

DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;
