-- Closes: https://github.com/ClickHouse/ClickHouse/issues/121401
-- Resolving JOIN USING identifiers from aliases defined in the WITH clause
-- under analyzer_compatibility_join_using_top_level_identifier = 1.

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS installs;

CREATE TABLE events   (platform String, idfv String, advertising_id String, event_date Date) ENGINE = Memory;
CREATE TABLE installs (platform String, idfv String, advertising_id String, event_date Date) ENGINE = Memory;
INSERT INTO events   VALUES ('ios', 'AAA', '', '2026-06-02'), ('android', '', 'bbb', '2026-06-03');
INSERT INTO installs VALUES ('ios', 'aaa', '', '2026-06-01'), ('android', '', 'bbb', '2026-06-01');

SET joined_subquery_requires_alias = 0;

-- case 1: the query from the issue. Default (setting = 0) cannot resolve the alias.
SET analyzer_compatibility_join_using_top_level_identifier = 0;
WITH lower(if(platform = 'ios', idfv, advertising_id)) AS id, dateDiff('day', InstallDate, event_date) AS lifetime
SELECT lifetime, count() AS users
FROM events
INNER JOIN (WITH lower(if(platform = 'ios', idfv, advertising_id)) AS id SELECT id, event_date AS InstallDate FROM installs) USING (id)
GROUP BY lifetime ORDER BY lifetime; -- { serverError UNKNOWN_IDENTIFIER }

SET analyzer_compatibility_join_using_top_level_identifier = 1;
WITH lower(if(platform = 'ios', idfv, advertising_id)) AS id, dateDiff('day', InstallDate, event_date) AS lifetime
SELECT lifetime, count() AS users
FROM events
INNER JOIN (WITH lower(if(platform = 'ios', idfv, advertising_id)) AS id SELECT id, event_date AS InstallDate FROM installs) USING (id)
GROUP BY lifetime ORDER BY lifetime;

DROP TABLE events;
DROP TABLE installs;

-- case 2: minimal WITH alias.
SET analyzer_compatibility_join_using_top_level_identifier = 1;
WITH x + 1 AS id SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id);
SET analyzer_compatibility_join_using_top_level_identifier = 0;
WITH x + 1 AS id SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id); -- { serverError UNKNOWN_IDENTIFIER }

SET analyzer_compatibility_join_using_top_level_identifier = 1;

-- case 3: the key is referenced in the SELECT list as well.
WITH x + 1 AS id SELECT id, x FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id);

-- case 4: WITH alias takes priority over a real left column (old-analyzer-compatible).
WITH x + 10 AS id SELECT sum(x) FROM (SELECT 1 AS x, 2 AS id) t1 JOIN (SELECT 2 AS id) t2 USING (id);
SET analyzer_compatibility_join_using_top_level_identifier = 0;
WITH x + 10 AS id SELECT sum(x) FROM (SELECT 1 AS x, 2 AS id) t1 JOIN (SELECT 2 AS id) t2 USING (id);
SET analyzer_compatibility_join_using_top_level_identifier = 1;

-- case 5: WITH alias of a plain left column.
WITH x AS id SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN (SELECT 1 AS id) t2 USING (id);

-- case 6: WITH alias expression references a column absent from the left table.
WITH y + 1 AS id SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id, 1 AS y) t2 USING (id); -- { serverError UNKNOWN_IDENTIFIER }

-- case 7: duplicated WITH alias with different expressions must not be picked arbitrarily.
WITH x + 1 AS id, x + 2 AS id SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id); -- { serverError UNKNOWN_IDENTIFIER }
WITH x + 1 AS id SELECT sum(x + 2 AS id) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id); -- { serverError UNKNOWN_IDENTIFIER }

-- case 7b: duplicated aliases with the same expression are accepted (old-analyzer-compatible).
WITH x + 1 AS id, x + 1 AS id SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id);
WITH x + 1 AS id SELECT sum(x + 1 AS id) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id);
SELECT sum(x + 1 AS id) + sum(x + 1 AS id) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id);

-- case 8: a lambda alias must not become a USING key, the real left column is used.
WITH (x -> x + 1) AS id SELECT sum(x) FROM (SELECT 1 AS x, 3 AS id) t1 JOIN (SELECT 3 AS id) t2 USING (id);

-- case 9: a CTE alongside the alias.
WITH t2 AS (SELECT 2 AS id), x + 1 AS id SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN t2 USING (id);

-- case 10: alias nested inside a WITH expression.
WITH (x + 1 AS id) * 2 AS doubled SELECT sum(doubled) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id);

-- case 11: legacy WITH scoping. The right side is a table: in this mode the WITH alias is visible
-- in a right-side subquery too and would conflict with a projection alias of the same name there.
DROP TABLE IF EXISTS t_right;
CREATE TABLE t_right (id UInt8) ENGINE = Memory;
INSERT INTO t_right VALUES (2);
SET enable_scopes_for_with_statement = 0;
WITH x + 1 AS id SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN t_right USING (id);
SET enable_scopes_for_with_statement = 1;
DROP TABLE t_right;

-- case 12: the JOIN is in a subquery with its own WITH.
SELECT max(s) FROM (WITH x + 1 AS id SELECT sum(x) AS s FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id));

-- case 13: LEFT JOIN with join_use_nulls.
WITH x + 1 AS id SELECT x, t2.id FROM (SELECT 1 AS x UNION ALL SELECT 5) t1 LEFT JOIN (SELECT 2 AS id) t2 USING (id) ORDER BY x SETTINGS join_use_nulls = 1;

-- Queries sent to remote servers and parallel replicas.
DROP TABLE IF EXISTS t_local;
CREATE TABLE t_local (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_local VALUES (1);

-- R1: the WITH section is not in the shipped SQL, so the key is rejected on the initiator.
WITH x + 1 AS id SELECT sum(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_local) AS t1 JOIN (SELECT 2 AS id) t2 USING (id); -- { serverError UNSUPPORTED_METHOD }

-- R2: the key is also a top-level SELECT-list column, so the shipped SQL carries the alias and the shard re-resolves it.
WITH x + 1 AS id SELECT id, sum(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_local) AS t1 JOIN (SELECT 2 AS id) t2 USING (id) GROUP BY id;

-- P1: parallel replicas are silently downgraded, the query still returns.
WITH x + 1 AS id SELECT sum(x) FROM t_local AS t1 JOIN (SELECT 2 AS id) t2 USING (id)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- P2: force mode throws instead of downgrading.
WITH x + 1 AS id SELECT sum(x) FROM t_local AS t1 JOIN (SELECT 2 AS id) t2 USING (id)
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_local;
