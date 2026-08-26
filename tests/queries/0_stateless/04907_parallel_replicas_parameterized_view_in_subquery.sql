-- https://github.com/ClickHouse/ClickHouse/issues/106039
-- A parameterized view used as an IN-subquery survives into the query text sent to the
-- replicas, so a replica plans the view body while the initiator plans the outer read.
-- Both reads keyed on the same table announced to the same coordinator, which aborted
-- with "Coordination mode mismatch for stream <table>: got Default, expected WithOrder".

DROP TABLE IF EXISTS pv_data;
DROP TABLE IF EXISTS pv_other;
DROP VIEW IF EXISTS pv_same_table;
DROP VIEW IF EXISTS pv_other_table;

CREATE TABLE pv_data (id String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE pv_other (id String) ENGINE = MergeTree() ORDER BY id;
CREATE VIEW pv_same_table AS SELECT id FROM pv_data WHERE id = {id:String};
CREATE VIEW pv_other_table AS SELECT id FROM pv_other WHERE id = {id:String};

INSERT INTO pv_data VALUES ('a');
INSERT INTO pv_data VALUES ('b');
INSERT INTO pv_other VALUES ('a');
INSERT INTO pv_other VALUES ('b');

-- 2 rather than 1: at 1 an unsupported shape falls back to a plain local read and still
-- returns every row expected below, so a regression would pass unnoticed.
SET enable_parallel_replicas = 2, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1;
-- The randomizer varies all five of these, and each one alone stops the two reads from being
-- planned in different coordination modes: at automatic_parallel_replicas_mode = 2 or
-- enable_analyzer = 0 parallel replicas do not run, at optimize_read_in_order = 0 the outer
-- read is not in-order so both reads announce Default, and at parallel_replicas_local_plan = 0
-- the initiator contributes no in-order local plan.
SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET optimize_read_in_order = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

-- The view reads the same table as the outer query. No id is both 'a' and 'b'.
SELECT 'and same', id
FROM pv_data
WHERE (id IN (SELECT id FROM pv_same_table(id = 'a')))
  AND (id IN (SELECT id FROM pv_same_table(id = 'b')))
ORDER BY id SETTINGS log_comment = '04907_and_same';

SELECT 'or same', id
FROM pv_data
WHERE (id IN (SELECT id FROM pv_same_table(id = 'a')))
   OR (id IN (SELECT id FROM pv_same_table(id = 'b')))
ORDER BY id SETTINGS log_comment = '04907_or_same';

-- The view reads a different table, which the initiator never announced.
SELECT 'and other', id
FROM pv_data
WHERE (id IN (SELECT id FROM pv_other_table(id = 'a')))
  AND (id IN (SELECT id FROM pv_other_table(id = 'b')))
ORDER BY id SETTINGS log_comment = '04907_and_other';

SELECT 'or other', id
FROM pv_data
WHERE (id IN (SELECT id FROM pv_other_table(id = 'a')))
   OR (id IN (SELECT id FROM pv_other_table(id = 'b')))
ORDER BY id SETTINGS log_comment = '04907_or_other';

-- Plain IN-subqueries over the same table keep coordinating as before.
SELECT 'plain', id
FROM pv_data
WHERE (id IN (SELECT id FROM pv_data WHERE id = 'a'))
   OR (id IN (SELECT id FROM pv_data WHERE id = 'b'))
ORDER BY id SETTINGS log_comment = '04907_plain';

-- The rows above are also what a query returns when parallel replicas never engage, so assert
-- each query reached the coordinator at all. ParallelReplicasHandleAnnouncementMicroseconds is
-- incremented in ParallelReplicasReadingCoordinator::handleInitialAllRangesAnnouncement, which
-- is the function that raised the mismatch. This counts the initiator's own local plan too, so
-- it shows coordination happened, not which replica announced; the two-sided randomized run is
-- what shows the repaired path. ParallelReplicasHandleRequestMicroseconds is not usable here:
-- the two AND queries read no marks, so they never reach a read request. Prints 1 for every
-- query. enable_parallel_replicas = 0 keeps this query itself off the offloaded path, where
-- mode 2 throws on a shape it does not support instead of falling back.
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['ParallelReplicasHandleAnnouncementMicroseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('04907_and_same', '04907_or_same', '04907_and_other', '04907_or_other', '04907_plain')
  AND type = 'QueryFinish'
  AND query_id = initial_query_id
  AND event_time >= now() - INTERVAL 600 SECOND
ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

DROP VIEW pv_other_table;
DROP VIEW pv_same_table;
DROP TABLE pv_other;
DROP TABLE pv_data;
