-- Tests for storing the hash table payload of a hash join in row-major form.

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;
DROP TABLE IF EXISTS right_asof;

CREATE TABLE left (k Int64, t DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right (k Int64, v1 Nullable(Int64), v2 UInt8, s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right_asof (k Int64, t DateTime('UTC'), v2 Nullable(Int64), s String) ENGINE = MergeTree ORDER BY (k, t);

INSERT INTO left SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number FROM numbers(10);
INSERT INTO right SELECT number + 7, number, number, toString(number) FROM numbers(5);
INSERT INTO right VALUES (7, NULL, 5, 'dup');
INSERT INTO right_asof SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number, number, toString(number) FROM numbers(5);

SET join_algorithm = 'hash';

SELECT '--- Row store planner decision test ---';

 -- Pin planner settings
-- The row store is the feature under test and CI randomizes it off, which would make every
-- "Initialized Row store" assertion below read 0.
SET enable_hash_join_row_store = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 0; -- Disable statistics to force using the hint
SET min_rows_ratio_for_hash_join_row_store = 2;  -- Pin minimum join output to build size ratio

SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null
SETTINGS min_rows_ratio_for_hash_join_row_store = 0, log_comment = 'rs_always_enabled';

SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = 'rs_disabled_by_unknown_statistcs';

SET param__internal_join_table_stat_hints = '{"left": {"cardinality": 1000000, "distinct_keys": {"k": 1}}, "right": {"cardinality": 100, "distinct_keys": {"k": 1}}}';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null
SETTINGS log_comment = 'rs_enabled_by_planner';

SET param__internal_join_table_stat_hints = '{"left": {"cardinality": 1000000, "distinct_keys": {"k": 1000000}}, "right": {"cardinality": 100, "distinct_keys": {"k": 100}}}';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null
SETTINGS log_comment = 'rs_disabled_by_planner';

SYSTEM FLUSH LOGS text_log, query_log;

SELECT 'rs_always_enabled', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_always_enabled' AND type = 'QueryFinish');

SELECT 'rs_disabled_by_unknown_statistcs', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_disabled_by_unknown_statistcs' AND type = 'QueryFinish');

SELECT 'rs_enabled_by_planner', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_enabled_by_planner' AND type = 'QueryFinish');

SELECT 'rs_disabled_by_planner', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_disabled_by_planner' AND type = 'QueryFinish');

SELECT '--- Hash table matches feedback loop test ---';

-- Collect the number of hash table matches and feed it back into the row store decision on a following run.
SET collect_hash_table_stats_during_joins = 1;

-- The stat hints over-estimate the join output, so the first run enables the row store and records the actual number of matches. 
-- The second run uses the observed match count instead of the estimate and disables the row store.
SET param__internal_join_table_stat_hints = '{"left": {"cardinality": 1000000, "distinct_keys": {"k": 1}}, "right": {"cardinality": 100, "distinct_keys": {"k": 1}}}';

SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null
SETTINGS log_comment = 'rs_collect_runtime_stats';

SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null
SETTINGS log_comment = 'rs_disabed_by_runtime_stats';

SYSTEM FLUSH LOGS text_log, query_log;

SELECT 'rs_collect_runtime_stats', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_collect_runtime_stats' AND type = 'QueryFinish');

SELECT 'rs_disabed_by_runtime_stats', countIf(message LIKE 'Initialized Row store%') > 0
FROM system.text_log
WHERE event_date >= yesterday() AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'rs_disabed_by_runtime_stats' AND type = 'QueryFinish');

-- Keep the row store enabled regardless of cardinality estimates.
SET min_rows_ratio_for_hash_join_row_store = 0;

SELECT '--- INNER JOIN ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- LEFT JOIN ---';
SELECT * FROM left l LEFT JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- RIGHT JOIN ---';
SELECT * FROM left l RIGHT JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- FULL JOIN ---';
SELECT * FROM left l FULL JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- ASOF JOIN ---';
SELECT * FROM left l ASOF JOIN right_asof r ON l.k = r.k AND l.t >= r.t ORDER BY ALL;

SELECT '--- Parallel hash JOIN ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

SELECT '--- Parallel hash FULL JOIN (join_use_nulls) ---';
SELECT * FROM left l FULL JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;

SELECT '--- Row-list JOIN output ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_output_by_rowlist_perkey_rows_threshold = 0;

SELECT '--- Join with block splitting ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS max_joined_block_size_rows = 2, joined_block_split_single_row = 1;

SELECT '--- Grace hash JOIN ---';
CREATE TABLE grace_right (k UInt64, v1 Nullable(Int64), v2 UInt8, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO grace_right SELECT number, if(number % 10 = 0, NULL, number), number % 251, toString(number) FROM numbers(20000);

SELECT count(), countIf(r.v1 IS NULL), sum(r.v2), sum(length(r.s)) FROM (SELECT number % 20000 AS k FROM numbers(40000)) l INNER JOIN grace_right r ON l.k = r.k
SETTINGS join_algorithm = 'grace_hash', max_bytes_in_join = 100000;

DROP TABLE grace_right;
DROP TABLE right_asof;
DROP TABLE right;
DROP TABLE left;
