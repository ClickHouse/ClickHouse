-- Tags: no-parallel

DROP DATABASE IF EXISTS 03147_db;
CREATE DATABASE IF NOT EXISTS 03147_db;
CREATE TABLE 03147_db.t (n Int8) ENGINE=MergeTree ORDER BY n;
INSERT INTO 03147_db.t SELECT * FROM numbers(10);
USE 03147_db;

-- We use the old setting here just to make sure we preserve it as an alias.
SET allow_experimental_parallel_reading_from_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'parallel_replicas', max_parallel_replicas = 100;

SELECT * FROM loop(03147_db.t) LIMIT 15 FORMAT Null;
