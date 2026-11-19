-- Regression: a second optimization of a `Merge` view must preserve its join runtime filter.

SET enable_join_runtime_filters = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET optimize_move_to_prewhere = 1;

DROP TABLE IF EXISTS rf_merge;
DROP VIEW IF EXISTS rf_view;
DROP TABLE IF EXISTS rf_probe;
DROP TABLE IF EXISTS rf_build;

CREATE TABLE rf_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE rf_build (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rf_probe SELECT number, number FROM numbers(200000);
INSERT INTO rf_build SELECT number * 1000 FROM numbers(50);

CREATE VIEW rf_view AS SELECT p.k AS k, p.v AS v FROM rf_probe AS p INNER JOIN rf_build AS b ON p.k = b.k;
CREATE TABLE rf_merge (k UInt64, v UInt64) ENGINE = Merge(currentDatabase(), '^rf_view$');

SELECT count(), sum(v) FROM rf_merge SETTINGS log_comment = '05368_merge';
SELECT count(), sum(v) FROM rf_merge SETTINGS enable_join_runtime_filters = 0;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- The probe read allows only matches and bloom-filter false positives.
SELECT q.ProfileEvents['RuntimeFiltersCreated'], sum(p.output_rows) BETWEEN 50 AND 1000
FROM system.query_log AS q
INNER JOIN system.processors_profile_log AS p ON p.query_id = q.query_id
WHERE q.event_date >= yesterday() AND q.event_time >= now() - 600 AND q.type = 'QueryFinish' AND q.current_database = currentDatabase()
  AND q.log_comment = '05368_merge'
  AND p.event_date >= yesterday() AND p.event_time >= now() - 600 AND p.name LIKE 'MergeTreeSelect%' AND p.plan_step_description = currentDatabase() || '.rf_probe'
GROUP BY q.query_id, q.ProfileEvents['RuntimeFiltersCreated'];

DROP TABLE rf_merge;
DROP VIEW rf_view;
DROP TABLE rf_probe;
DROP TABLE rf_build;
