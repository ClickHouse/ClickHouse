-- Related: https://github.com/ClickHouse/clickhouse-private/issues/55715 (Cluster C, distributed/parallel replicas)
-- A nested-alias `USING` key cannot be re-resolved by a remote server (the rendered query loses the nested
-- alias), so parallel replicas are downgraded at analysis time and `Distributed`/`remote` shipping fails loudly.

SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;

CREATE TABLE events
(
    event_date Date, platform String, advertising_id String, idfv String,
    event_name String, event_revenue_usd String
) ENGINE = MergeTree ORDER BY event_date;
INSERT INTO events VALUES ('2024-01-01', 'android', 'aid1', '', 'install', '0'), ('2024-01-01', 'android', 'aid1', '', 'af_purchase', '5.0'), ('2024-01-02', 'ios', '', 'idfv1', 'install', '0'), ('2024-01-02', 'ios', '', 'idfv1', 'af_purchase', '3.0');

-- P1: parallel replicas silently downgraded (hook fires before any cluster lookup), so the query still returns.
SELECT uniqExact(lower(if(platform = 'android', advertising_id, idfv)) AS id) AS users_pay
FROM events AS iap
INNER JOIN (
    SELECT lower(if(platform = 'ios', idfv, advertising_id)) AS id, min(event_date) AS InstallDate
    FROM events WHERE event_name = 'install' GROUP BY id
) AS sub USING (id)
WHERE event_name LIKE 'af_%' AND toFloat64OrZero(event_revenue_usd) > 0
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

-- P2: force mode throws instead of downgrading.
SELECT uniqExact(lower(if(platform = 'android', advertising_id, idfv)) AS id) AS users_pay
FROM events AS iap
INNER JOIN (
    SELECT lower(if(platform = 'ios', idfv, advertising_id)) AS id, min(event_date) AS InstallDate
    FROM events WHERE event_name = 'install' GROUP BY id
) AS sub USING (id)
WHERE event_name LIKE 'af_%' AND toFloat64OrZero(event_revenue_usd) > 0
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', automatic_parallel_replicas_mode = 0; -- { serverError SUPPORT_IS_DISABLED }

-- R1: nested alias over `remote` stays a loud error (documented limitation).
SELECT uniqExact(lower(if(platform = 'android', advertising_id, idfv)) AS id) AS users_pay
FROM remote('127.0.0.{1,2}', currentDatabase(), events) AS iap
INNER JOIN (
    SELECT lower(if(platform = 'ios', idfv, advertising_id)) AS id, min(event_date) AS InstallDate
    FROM events WHERE event_name = 'install' GROUP BY id
) AS sub USING (id)
WHERE event_name LIKE 'af_%' AND toFloat64OrZero(event_revenue_usd) > 0; -- { serverError UNKNOWN_IDENTIFIER }

-- R2: the `join_use_nulls` LEFT variant over `remote`, same expectation.
SELECT uniqExact(lower(if(platform = 'android', advertising_id, idfv)) AS id) AS users_pay
FROM remote('127.0.0.{1,2}', currentDatabase(), events) AS iap
LEFT JOIN (SELECT lower(if(platform = 'ios', idfv, advertising_id)) AS id FROM events WHERE event_name = 'install' GROUP BY id) AS sub USING (id)
WHERE event_name LIKE 'af_%'
SETTINGS join_use_nulls = 1; -- { serverError UNKNOWN_IDENTIFIER }

-- T1: top-level aliases keep working over `remote` (control; no downgrade, no error).
-- Each left row is read once per shard, so the single-shard result is duplicated; ORDER BY makes it deterministic.
CREATE TABLE t1 (id String, val String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t2 (id String, code String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t3 (id String, code String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t1 VALUES ('a', 'v'), ('b', 'w');
INSERT INTO t2 VALUES ('b', 'c');
INSERT INTO t3 VALUES ('a_1', 'c'), ('b_1', 'd');

SELECT t2.id || '_1' AS id, t1.val
FROM remote('127.0.0.{1,2}', currentDatabase(), t1) AS t1
LEFT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 USING (id)
ORDER BY t1.val, id
SETTINGS join_use_nulls = 1;

DROP TABLE events;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
