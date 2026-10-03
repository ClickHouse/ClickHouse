-- INSERT ... SELECT with parallel replicas: a CTE or `grouping` in a subquery works on the replicas,
-- and a SELECT from a CTE or from a view the replicas do not read in coordination is inserted once, not once per replica.

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1,
    parallel_replicas_prefer_local_replica = 1, parallel_distributed_insert_select = 2, insert_deduplicate = 0,
    parallel_replicas_allow_materialized_views = 1, parallel_replicas_allow_view_over_mergetree = 0;

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS mv_tgt;
DROP TABLE IF EXISTS tv;
DROP TABLE IF EXISTS v_limit;
DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS dst_mem;
DROP TABLE IF EXISTS dst SYNC;
DROP TABLE IF EXISTS j74;
DROP TABLE IF EXISTS ids_merge_a;
DROP TABLE IF EXISTS ids;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src SELECT number FROM numbers(100);
CREATE TABLE ids (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ids VALUES (20), (21);
CREATE TABLE ids_merge_a (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ids_merge_a VALUES (50), (51);
CREATE TABLE j74 (id Int8, name String, value Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO j74 VALUES (1, 'a', 10), (2, 'b', 20);
CREATE TABLE dst (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst', 'r1') ORDER BY x;
CREATE TABLE dst_mem (x UInt64) ENGINE = Memory;
CREATE VIEW v AS SELECT x FROM src;
CREATE VIEW v_limit AS SELECT x FROM src ORDER BY x LIMIT 5;
CREATE TABLE tv AS view(SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.src);
CREATE TABLE mv_tgt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mv_tgt SELECT 1000 + number FROM numbers(10);
CREATE MATERIALIZED VIEW mv TO mv_tgt AS SELECT x FROM src;

SELECT '-- inserted by the replicas';
INSERT INTO dst WITH t AS (SELECT arrayJoin([1, 2]) AS id) SELECT x FROM src WHERE x IN (SELECT id FROM t) SETTINGS log_comment = 'arm_cte';
INSERT INTO dst WITH RECURSIVE r AS (SELECT 10 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 11) SELECT x FROM src WHERE x IN (SELECT n FROM r) SETTINGS log_comment = 'arm_recursive';
INSERT INTO dst SELECT x FROM src WHERE x IN (SELECT id FROM ids GROUP BY ROLLUP(id) HAVING grouping(id) = 0) SETTINGS log_comment = 'arm_grouping';
INSERT INTO dst WITH t(k) AS (SELECT arrayJoin([30, 31])) SELECT x FROM src WHERE x IN (SELECT k FROM t) SETTINGS log_comment = 'arm_cte_columns';
INSERT INTO dst WITH t AS (SELECT arrayJoin([40, 41]) AS id) SELECT x FROM src WHERE x IN (SELECT id FROM t) SETTINGS log_comment = 'arm_cte_no_local_plan', parallel_replicas_local_plan = 0;
INSERT INTO dst WITH t AS (SELECT id FROM merge('^ids_merge_')) SELECT x FROM src WHERE x IN (SELECT id FROM t) SETTINGS log_comment = 'arm_cte_merge';
INSERT INTO dst SELECT x FROM src WHERE x IN (SELECT id + 10 FROM merge('^ids_merge_')) SETTINGS log_comment = 'arm_merge';
INSERT INTO dst WITH c AS (SELECT * FROM (SELECT * FROM j74) ANY LEFT JOIN (SELECT * FROM j74) USING id)
    SELECT x FROM src WHERE x IN (SELECT toUInt64(value) FROM c)
    SETTINGS joined_subquery_requires_alias = 0, log_comment = 'arm_cte_join_star';
INSERT INTO dst SELECT x FROM src WHERE x IN (SELECT toUInt64(value) FROM (SELECT * FROM (SELECT * FROM j74) ANY LEFT JOIN (SELECT * FROM j74) USING id))
    SETTINGS joined_subquery_requires_alias = 0, log_comment = 'arm_join_star';
SELECT groupArray(x) FROM (SELECT x FROM dst ORDER BY x);

SELECT '-- inserted once';
TRUNCATE TABLE dst;
INSERT INTO dst SELECT x FROM v SETTINGS log_comment = 'arm_view';
SELECT count(), sum(x) FROM dst;
TRUNCATE TABLE dst;
INSERT INTO dst SELECT x FROM v_limit SETTINGS log_comment = 'arm_view_limit';
SELECT count(), sum(x) FROM dst;
TRUNCATE TABLE dst;
INSERT INTO dst SELECT x FROM tv SETTINGS log_comment = 'arm_view_proxy';
SELECT count(), sum(x) FROM dst;
TRUNCATE TABLE dst;
INSERT INTO dst SELECT x FROM v SETTINGS log_comment = 'arm_view_named_over_mergetree', parallel_replicas_allow_view_over_mergetree = 1;
SELECT count(), sum(x) FROM dst;
INSERT INTO dst_mem WITH c AS (SELECT DISTINCT x % 7 AS y FROM src) SELECT y FROM c SETTINGS log_comment = 'arm_from_cte';
SELECT count(), sum(x) FROM dst_mem;

SELECT '-- a materialized view is read through its target table, so the replicas insert it';
TRUNCATE TABLE dst;
INSERT INTO dst SELECT x FROM mv SETTINGS log_comment = 'arm_materialized_view';
SELECT count(), sum(x) FROM dst;

SELECT '-- a view the replicas read in coordination is inserted by the replicas';
INSERT INTO FUNCTION null('x UInt64') SELECT x FROM v SETTINGS log_comment = 'arm_view_over_mergetree', parallel_replicas_allow_view_over_mergetree = 1;
INSERT INTO FUNCTION null('x UInt64') SELECT x FROM v_limit SETTINGS log_comment = 'arm_view_limit_over_mergetree', parallel_replicas_allow_view_over_mergetree = 1;

SELECT '-- INSERT queries run by the replicas';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, countIf(is_initial_query = 0)
FROM system.query_log
WHERE (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish' AND query_kind = 'Insert' AND startsWith(log_comment, 'arm_')
GROUP BY log_comment
ORDER BY log_comment;

SELECT '-- rows written for the views read in coordination';
SELECT log_comment, countIf(is_initial_query = 0), sum(written_rows)
FROM system.query_log
WHERE (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment IN ('arm_view_limit_over_mergetree', 'arm_view_over_mergetree')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mv;
DROP TABLE mv_tgt;
DROP TABLE tv;
DROP TABLE v_limit;
DROP TABLE v;
DROP TABLE dst_mem;
DROP TABLE dst SYNC;
DROP TABLE j74;
DROP TABLE ids_merge_a;
DROP TABLE ids;
DROP TABLE src;
