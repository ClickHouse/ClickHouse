-- Reproduces "Not-ready Set is passed as the second argument for function 'globalNotNullIn'"
-- from stress test with parallel replicas on GLOBAL NOT IN queries.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=d0432097aed783bd35054dce2edcefe0c4e5122c&name_0=MasterCI&name_1=Stress%20test%20%28amd_tsan%29
-- Tags: replica

DROP TABLE IF EXISTS null_in_pr;
CREATE TABLE null_in_pr (dt DateTime, idx Int32, i Nullable(UInt64)) ENGINE = MergeTree() PARTITION BY dt ORDER BY idx;
INSERT INTO null_in_pr SELECT number % 3, number, number FROM system.numbers LIMIT 99999;
INSERT INTO null_in_pr VALUES (0, 123456780, NULL);
INSERT INTO null_in_pr VALUES (1, 123456781, NULL);

SET transform_null_in = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_local_plan = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;

SELECT count() == 66668 FROM null_in_pr WHERE i global not in (SELECT i FROM null_in_pr WHERE dt = 2);
SELECT count() == 33333 FROM null_in_pr WHERE i global in (SELECT i FROM null_in_pr WHERE dt = 2);
SELECT count() == 66666 FROM null_in_pr WHERE i global not in (SELECT i FROM null_in_pr WHERE dt = 1);
SELECT count() == 33335 FROM null_in_pr WHERE i global in (SELECT i FROM null_in_pr WHERE dt = 0);

DROP TABLE null_in_pr;
