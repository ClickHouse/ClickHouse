-- Tags: no-fasttest
-- Tag no-fasttest: needs the test clusters

-- The replicas of a cluster table function re-analyze the query text they receive, so it has to contain the
-- definitions of the CTEs it uses and must not contain analyzer-internal names.

INSERT INTO FUNCTION file(currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') SELECT number FROM numbers(4) SETTINGS engine_file_truncate_on_insert = 1;

WITH t AS (SELECT arrayJoin([2, 3]) AS id)
SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') WHERE n IN (SELECT id FROM t);
WITH t AS (SELECT arrayJoin([2, 3]) AS id)
SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') WHERE n IN t;
WITH t AS (SELECT 2 AS id UNION ALL SELECT 3)
SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') WHERE n NOT IN (SELECT id FROM t);
SELECT sum(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') WHERE n IN (WITH t AS (SELECT arrayJoin([2, 3]) AS id) SELECT id FROM t);
WITH t AS (SELECT arrayJoin([2, 3]) AS id)
SELECT sum(n), count() FROM (SELECT n FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') WHERE n IN (SELECT id FROM t));

DROP TABLE IF EXISTS dst;
CREATE TABLE dst (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO dst WITH t AS (SELECT arrayJoin([2, 3]) AS id) SELECT n FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') WHERE n IN (SELECT id FROM t);
SELECT sum(n), count() FROM dst;
DROP TABLE dst;

SELECT n, grouping(n), count() FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') GROUP BY ROLLUP(n) ORDER BY n, 2;
SELECT *, n + 10 AS n FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05293.tsv', 'TSV', 'n UInt64') ORDER BY 1;
