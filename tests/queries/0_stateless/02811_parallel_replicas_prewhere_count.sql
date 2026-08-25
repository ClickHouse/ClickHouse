DROP TABLE IF EXISTS users;
CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree() ORDER BY uid;

INSERT INTO users VALUES (111, 'JFK', 33);
INSERT INTO users VALUES (6666, 'KLM', 48);
INSERT INTO users VALUES (88888, 'AMS', 50);

SELECT '-- count() ------------------------------';
SELECT count() FROM users PREWHERE uid > 2000;

-- enable parallel replicas but with high rows threshold
SET
skip_unavailable_shards=1,
enable_parallel_replicas=1,
max_parallel_replicas=3,
cluster_for_parallel_replicas='parallel_replicas',
parallel_replicas_for_non_replicated_merge_tree=1,
parallel_replicas_min_number_of_rows_per_replica=1000;

SELECT '-- count() with parallel replicas -------';
SELECT count() FROM users PREWHERE uid > 2000;

DROP TABLE users;
