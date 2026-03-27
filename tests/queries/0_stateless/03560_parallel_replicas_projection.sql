-- Tags: long

DROP TABLE IF EXISTS normal;
CREATE TABLE IF NOT EXISTS normal
(
    `key` UInt32,
    `value` UInt32,
)
ENGINE = MergeTree
ORDER BY tuple() settings index_granularity=1;

SYSTEM STOP MERGES normal;

INSERT INTO normal select number as key, number as value from numbers(10000);
ALTER TABLE normal ADD PROJECTION p_normal (SELECT key, value ORDER BY key);
INSERT INTO normal select number as key, number as value from numbers(10000, 100);

SET parallel_replicas_only_with_analyzer = 0;
SET optimize_use_projections = 1, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 2, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT '---normal : contains both projections and parts ---';
SELECT trimLeft(replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')) FROM (explain SELECT sum(key) FROM normal WHERE key > 9999 AND key < 10010) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%ReadFromRemoteParallelReplicas%' SETTINGS enable_analyzer = 1;
SELECT sum(key) FROM normal WHERE key > 9999 AND key < 10010;

SELECT '---normal : contains only projections ---';
TRUNCATE TABLE normal;
INSERT INTO normal select number as key, number as value from numbers(10100);

SELECT trimLeft(replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')) FROM (explain SELECT sum(key) FROM normal WHERE key > 9999 AND key < 10010) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%ReadFromRemoteParallelReplicas%' SETTINGS enable_analyzer = 1;
SELECT sum(key) FROM normal WHERE key > 9999 AND key < 10010;

DROP TABLE normal;

DROP TABLE IF EXISTS agg;
CREATE TABLE agg
(
    `key` UInt32,
    `value` UInt32,
)
ENGINE = MergeTree
ORDER BY tuple() settings index_granularity=1;

SYSTEM STOP MERGES agg;

INSERT INTO agg SELECT number AS key, number AS value FROM numbers(100);
ALTER TABLE agg ADD PROJECTION p_agg (SELECT key, sum(value) GROUP BY key);
INSERT INTO agg SELECT number AS key, number AS value FROM numbers(100, 100);

SELECT '---agg : contains both projections and parts ---';
SELECT trimLeft(replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')) FROM (explain SELECT sum(value) AS v FROM agg where key > 90 AND key < 110) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%ReadFromRemoteParallelReplicas%' SETTINGS enable_analyzer = 1;
SELECT sum(value) AS v FROM agg where key > 90 AND key < 110;

SELECT '---agg : contains only projections ---';
TRUNCATE TABLE agg;
INSERT INTO agg SELECT number AS key, number AS value FROM numbers(200);

SELECT trimLeft(replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')) FROM (explain SELECT sum(value) AS v FROM agg where key > 90 AND key < 110) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%ReadFromRemoteParallelReplicas%' SETTINGS enable_analyzer = 1;
SELECT sum(value) AS v FROM agg where key > 90 AND key < 110;
DROP TABLE agg;

DROP TABLE IF EXISTS x;
CREATE TABLE x (i int) engine MergeTree ORDER BY i SETTINGS index_granularity = 3;

INSERT INTO x SELECT * FROM numbers(10);

SELECT '--- min-max projection ---';

SELECT trimLeft(replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')) FROM (explain SELECT max(i) FROM x) WHERE explain LIKE '%ReadFromPreparedSource%' OR explain LIKE '%ReadFromRemoteParallelReplicas%' SETTINGS enable_analyzer = 1;
SELECT max(i) FROM x SETTINGS enable_analyzer = 1, max_rows_to_read = 2, optimize_use_implicit_projections = 1, optimize_use_projection_filtering = 1;

SELECT '--- exact-count projection ---';
SELECT trimLeft(replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')) FROM (explain SELECT count() FROM x WHERE (i >= 3 AND i <= 6) OR i = 7) WHERE explain LIKE '%ReadFromPreparedSource%' OR explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%ReadFromRemoteParallelReplicas%' SETTINGS enable_analyzer = 1;
SELECT count() FROM x WHERE (i >= 3 AND i <= 6) OR i = 7;
