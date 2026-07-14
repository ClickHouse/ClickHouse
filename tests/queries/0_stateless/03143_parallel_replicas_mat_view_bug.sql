DROP TABLE IF EXISTS mv_table;
DROP TABLE IF EXISTS null_table;

SET cluster_for_parallel_replicas='parallel_replicas', max_parallel_replicas=4, enable_parallel_replicas=1;
SET enable_analyzer=1;

CREATE TABLE null_table (str String) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_table (str String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03143_parallel_replicas_mat_view_bug', '{replica}') ORDER BY str AS SELECT str AS str FROM null_table;
INSERT INTO null_table VALUES ('test');

SELECT * FROM mv_table;
