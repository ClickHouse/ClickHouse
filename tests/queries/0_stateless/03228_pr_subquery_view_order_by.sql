DROP TABLE IF EXISTS view1;
DROP TABLE IF EXISTS table1;
CREATE TABLE table1 (number UInt64) ENGINE=MergeTree ORDER BY number SETTINGS index_granularity=1;
INSERT INTO table1 SELECT number FROM numbers(1, 300);
CREATE VIEW view1 AS SELECT number FROM table1;

SELECT *
FROM
(
    SELECT *
    FROM view1
)
ORDER BY number DESC
LIMIT 20
SETTINGS cluster_for_parallel_replicas = 'parallel_replicas', allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1;

DROP TABLE view1;
DROP TABLE table1;
