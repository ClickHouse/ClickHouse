DROP TABLE IF EXISTS local_table_l;
DROP TABLE IF EXISTS local_table_r;
DROP TABLE IF EXISTS dis_table_r;

CREATE TABLE local_table_l
(
    `c` Int32
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

CREATE TABLE local_table_r
(
    `c` Int32
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

CREATE TABLE dis_table_r
(
    `c` Int32
)
ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 'local_table_r');

SET parallel_replicas_only_with_analyzer=0;
SET serialize_query_plan=0, parallel_replicas_mark_segment_size=1, max_threads=1;
SET enable_parallel_replicas=1, parallel_replicas_local_plan=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

INSERT INTO local_table_l SELECT number AS c FROM numbers(10000);
INSERT INTO local_table_r SELECT number AS c FROM numbers(10000);

SELECT count() FROM local_table_l AS l RIGHT JOIN dis_table_r AS r ON l.c = r.c;

DROP TABLE local_table_l;
DROP TABLE local_table_r;
DROP TABLE dis_table_r;
