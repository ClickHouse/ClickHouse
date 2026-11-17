-- A SETTINGS clause inside a view definition body must be honored when deciding whether the
-- underlying MergeTree table is eligible for parallel replicas. When it was ignored, a view whose
-- body opts out of parallel replicas was still read through the coordinated remote path while each
-- replica read the whole table locally, so the query returned max_parallel_replicas times too many
-- rows.
--
-- Both parallel_replicas_for_non_replicated_merge_tree and parallel_replicas_allow_view_over_mergetree
-- carry that opt-out, the latter only once the body reads a further view.
--
-- index_granularity = 1 makes the inflation deterministic: every replica receives marks, so a
-- broken run returns exactly rows * max_parallel_replicas.

DROP VIEW IF EXISTS v_plain;
DROP VIEW IF EXISTS v_set;
DROP VIEW IF EXISTS v_nested;
DROP VIEW IF EXISTS v_union;
DROP VIEW IF EXISTS v_union_both;
DROP VIEW IF EXISTS v_union_second;
DROP VIEW IF EXISTS v_final;
DROP VIEW IF EXISTS v_plain_over_plain;
DROP VIEW IF EXISTS v_av_top;
DROP VIEW IF EXISTS v_av_body;
DROP VIEW IF EXISTS v_av_leaf;
DROP TABLE IF EXISTS mt_g;
DROP TABLE IF EXISTS mt_g2;
DROP TABLE IF EXISTS rmt_g;

CREATE TABLE mt_g (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE mt_g2 (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE rmt_g (a Int32, v Int32) ENGINE = ReplacingMergeTree(v) ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO mt_g SELECT number FROM numbers(300);
INSERT INTO mt_g2 SELECT number FROM numbers(300);
INSERT INTO rmt_g SELECT number, 1 FROM numbers(300);

CREATE VIEW v_plain AS SELECT a FROM mt_g;
CREATE VIEW v_set AS SELECT a FROM mt_g SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;
CREATE VIEW v_nested AS SELECT a FROM v_set;
CREATE VIEW v_union AS
    SELECT a FROM mt_g SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0
    UNION ALL
    SELECT a FROM mt_g2;
CREATE VIEW v_union_both AS
    SELECT a FROM mt_g SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0
    UNION ALL
    SELECT a FROM mt_g2 SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;
CREATE VIEW v_union_second AS
    SELECT a FROM mt_g
    UNION ALL
    SELECT a FROM mt_g2 SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;
CREATE VIEW v_final AS SELECT a FROM rmt_g FINAL;
CREATE VIEW v_av_leaf AS SELECT a FROM mt_g;
CREATE VIEW v_av_body AS SELECT a FROM v_av_leaf SETTINGS parallel_replicas_allow_view_over_mergetree = 0;
CREATE VIEW v_av_top AS SELECT a FROM v_av_body;
CREATE VIEW v_plain_over_plain AS SELECT a FROM v_av_leaf;

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_view_over_mergetree = 1;

-- parallel_replicas_local_plan is deliberately left to the runner's randomization: every
-- assertion below was measured identical at both values.

SELECT '-- ground truth, parallel replicas off';
SELECT count() FROM v_plain SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM v_set SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM v_union SETTINGS enable_parallel_replicas = 0;

SELECT '-- values with parallel replicas on';
SELECT count() FROM v_plain;
SELECT count() FROM v_set;
SELECT count() FROM v_nested;
SELECT count() FROM v_union;
SELECT count() FROM v_union_both;
SELECT count() FROM v_union_second;
SELECT count() FROM v_final;
SELECT count() FROM v_av_body;
SELECT count() FROM v_av_top;
SELECT count() FROM v_plain_over_plain;

-- Which relation is shipped to the replicas. An empty array means the query is not read through
-- the coordinated remote path at all, which is the expected verdict once a view body opts out.
SELECT '-- what is shipped to replicas';

SELECT 'v_plain', arraySort(groupUniqArray(if(explain LIKE '%v_plain%', 'v_plain', if(explain LIKE '%mt_g%', 'mt_g', 'other'))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_plain))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT 'v_set', arraySort(groupUniqArray(if(explain LIKE '%v_set%', 'v_set', if(explain LIKE '%mt_g%', 'mt_g', 'other'))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_set))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT 'v_nested', arraySort(groupUniqArray(if(explain LIKE '%v_nested%', 'v_nested', if(explain LIKE '%v_set%', 'v_set', if(explain LIKE '%mt_g%', 'mt_g', 'other')))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_nested))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT 'v_union', arraySort(groupUniqArray(if(explain LIKE '%v_union%', 'v_union', if(explain LIKE '%mt_g2%', 'mt_g2', if(explain LIKE '%mt_g%', 'mt_g', 'other')))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_union))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT 'v_union_both', arraySort(groupUniqArray(if(explain LIKE '%v_union_both%', 'v_union_both', if(explain LIKE '%mt_g2%', 'mt_g2', if(explain LIKE '%mt_g%', 'mt_g', 'other')))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_union_both))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT 'v_final', arraySort(groupUniqArray(if(explain LIKE '%v_final%', 'v_final', if(explain LIKE '%rmt_g%', 'rmt_g', 'other'))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_final))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT 'v_av_body', arraySort(groupUniqArray(if(explain LIKE '%v_av_body%', 'v_av_body', if(explain LIKE '%v_av_leaf%', 'v_av_leaf', if(explain LIKE '%mt_g%', 'mt_g', 'other')))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_av_body))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT 'v_av_top', arraySort(groupUniqArray(if(explain LIKE '%v_av_top%', 'v_av_top', if(explain LIKE '%v_av_body%', 'v_av_body', if(explain LIKE '%mt_g%', 'mt_g', 'other')))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_av_top))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

SELECT 'v_plain_over_plain', arraySort(groupUniqArray(if(explain LIKE '%v_plain_over_plain%', 'v_plain_over_plain', if(explain LIKE '%v_av_leaf%', 'v_av_leaf', if(explain LIKE '%mt_g%', 'mt_g', 'other')))))
FROM viewExplain('EXPLAIN', '', (SELECT count() FROM v_plain_over_plain))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

DROP VIEW v_plain;
DROP VIEW v_set;
DROP VIEW v_nested;
DROP VIEW v_union;
DROP VIEW v_union_both;
DROP VIEW v_union_second;
DROP VIEW v_final;
DROP VIEW v_plain_over_plain;
DROP VIEW v_av_top;
DROP VIEW v_av_body;
DROP VIEW v_av_leaf;
DROP TABLE mt_g;
DROP TABLE mt_g2;
DROP TABLE rmt_g;
