-- Tags: long

-- Both parallel replicas implementations are checked: the step of query-based parallel replicas is `ReadFromRemoteParallelReplicas`,
-- and the step of plan-based ones is `ReadFromParallelReplicas`, whose description is a multi-line dump of the shipped plan fragment.
-- Each step, with its description, is collapsed into a single `ReadFromRemoteParallelReplicas` line.

-- Statistics-based part pruning can skip parts before they appear in EXPLAIN output
SET use_statistics_for_part_pruning = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET explain_query_plan_default = 'legacy';

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

SET automatic_parallel_replicas_mode = 0;
SET optimize_use_projections = 1, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 2, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT '---normal : contains both projections and parts ---';
SELECT trimLeft(line) FROM (SELECT arrayJoin(splitByChar('\n', replaceRegexpAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFrom(Remote)?ParallelReplicas \\((?s:.*?)Replicas: [^\\n]*\\)', 'ReadFromRemoteParallelReplicas'))) AS line FROM (explain SELECT sum(key) FROM normal WHERE key > 9999 AND key < 10010)) WHERE line LIKE '%ReadFromMergeTree%' OR line LIKE '%ReadFromRemoteParallelReplicas%';
SELECT sum(key) FROM normal WHERE key > 9999 AND key < 10010;

SELECT '---normal : contains only projections ---';
TRUNCATE TABLE normal;
INSERT INTO normal select number as key, number as value from numbers(10100);

SELECT trimLeft(line) FROM (SELECT arrayJoin(splitByChar('\n', replaceRegexpAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFrom(Remote)?ParallelReplicas \\((?s:.*?)Replicas: [^\\n]*\\)', 'ReadFromRemoteParallelReplicas'))) AS line FROM (explain SELECT sum(key) FROM normal WHERE key > 9999 AND key < 10010)) WHERE line LIKE '%ReadFromMergeTree%' OR line LIKE '%ReadFromRemoteParallelReplicas%';
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
SELECT trimLeft(line) FROM (SELECT arrayJoin(splitByChar('\n', replaceRegexpAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFrom(Remote)?ParallelReplicas \\((?s:.*?)Replicas: [^\\n]*\\)', 'ReadFromRemoteParallelReplicas'))) AS line FROM (explain SELECT sum(value) AS v FROM agg where key > 90 AND key < 110)) WHERE line LIKE '%ReadFromMergeTree%' OR line LIKE '%ReadFromRemoteParallelReplicas%';
SELECT sum(value) AS v FROM agg where key > 90 AND key < 110;

SELECT '---agg : contains only projections ---';
TRUNCATE TABLE agg;
INSERT INTO agg SELECT number AS key, number AS value FROM numbers(200);

SELECT trimLeft(line) FROM (SELECT arrayJoin(splitByChar('\n', replaceRegexpAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFrom(Remote)?ParallelReplicas \\((?s:.*?)Replicas: [^\\n]*\\)', 'ReadFromRemoteParallelReplicas'))) AS line FROM (explain SELECT sum(value) AS v FROM agg where key > 90 AND key < 110)) WHERE line LIKE '%ReadFromMergeTree%' OR line LIKE '%ReadFromRemoteParallelReplicas%';
SELECT sum(value) AS v FROM agg where key > 90 AND key < 110;
DROP TABLE agg;

DROP TABLE IF EXISTS x;
CREATE TABLE x (i int) engine MergeTree ORDER BY i SETTINGS index_granularity = 3;

INSERT INTO x SELECT * FROM numbers(10);

SELECT '--- min-max projection ---';

SELECT trimLeft(line) FROM (SELECT arrayJoin(splitByChar('\n', replaceRegexpAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFrom(Remote)?ParallelReplicas \\((?s:.*?)Replicas: [^\\n]*\\)', 'ReadFromRemoteParallelReplicas'))) AS line FROM (explain SELECT max(i) FROM x)) WHERE line LIKE '%ReadFromPreparedSource%' OR line LIKE '%ReadFromRemoteParallelReplicas%';
SELECT max(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 1, optimize_use_projection_filtering = 1;

SELECT '--- exact-count projection ---';
SELECT trimLeft(line) FROM (SELECT arrayJoin(splitByChar('\n', replaceRegexpAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFrom(Remote)?ParallelReplicas \\((?s:.*?)Replicas: [^\\n]*\\)', 'ReadFromRemoteParallelReplicas'))) AS line FROM (explain SELECT count() FROM x WHERE (i >= 3 AND i <= 6) OR i = 7)) WHERE line LIKE '%ReadFromPreparedSource%' OR line LIKE '%ReadFromMergeTree%' OR line LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM x WHERE (i >= 3 AND i <= 6) OR i = 7;
