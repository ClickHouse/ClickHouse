DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test1_distributed;
DROP TABLE IF EXISTS test_merge;

SET enable_optimize_predicate_expression = 1;

CREATE TABLE test1 (id Int64, name String) ENGINE MergeTree PARTITION BY (id) ORDER BY (id);
CREATE TABLE test1_distributed AS test1 ENGINE = Distributed(test_cluster_two_shards_localhost, default, test1);
CREATE TABLE test_merge AS test1 ENGINE = Merge('default', 'test1_distributed');

SELECT count() FROM test_merge
JOIN (SELECT 'anystring' AS name) AS n
USING name
WHERE id = 1;

DROP TABLE test1;
DROP TABLE test_merge;


CREATE TABLE test1 (id Int64, name String) ENGINE MergeTree PARTITION BY (id) ORDER BY (id);
CREATE TABLE test_merge AS test1 ENGINE = Merge('default', 'test1');

SELECT count() FROM test_merge
JOIN (SELECT 'anystring' AS name) AS n
USING name
WHERE id = 1;

DROP TABLE test1;
DROP TABLE test_merge;
DROP TABLE test1_distributed;
