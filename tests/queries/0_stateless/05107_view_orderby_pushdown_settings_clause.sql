-- Tags: distributed

-- `pushOrderByIntoView` injects the outer `ORDER BY ... LIMIT` into the stored query of a view over
-- a `Distributed` table, so that the shards sort and truncate and the coordinator only merges. The
-- view's own `SETTINGS` clause is applied to the context that stored query runs in, so a clause
-- which constrains the rows the view exposes - a `limit`, an extra filter, `final` - must disable
-- the pushdown: the injected inner `LIMIT` would keep the wrong rows and the setting would then
-- drop them. Only a clause of pure execution tuning keeps the optimization, proven by the same
-- allowlist `StorageView::canHideRows` applies (`StorageView::settingsClauseCanHideRows`).

SET enable_analyzer = 1;

-- Target `pushOrderByIntoView` and not the unrelated trivial-view pushdown to `Distributed`.
SET optimize_trivial_view_pushdown_to_distributed = 0;

DROP TABLE IF EXISTS local_05107;
DROP TABLE IF EXISTS dist_05107;

CREATE TABLE local_05107 (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY (id, ts);
INSERT INTO local_05107 SELECT number, now() - number FROM numbers(100);

CREATE TABLE dist_05107 AS local_05107
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_05107, id);

-- No `SETTINGS` clause at all: the pushdown fires. This is the control which makes every
-- "disables pushdown" line below non-vacuous.
CREATE VIEW view_plain_05107 AS SELECT id, ts FROM dist_05107;

SELECT 'no SETTINGS clause keeps pushdown:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM view_plain_05107 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- A clause of pure execution tuning cannot change which rows the view returns, so the pushdown
-- must survive it.
CREATE VIEW view_tuning_05107 AS SELECT id, ts FROM dist_05107 SETTINGS max_threads = 1, max_block_size = 8192;

SELECT 'tuning-only SETTINGS clause keeps pushdown:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM view_tuning_05107 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- `additional_result_filter` grows a filter above the stored query's result, which
-- `IInterpreterUnionOrSelectQuery::addAdditionalPostFilter` applies only after that plan is built -
-- after the injected inner `LIMIT` has already decided which rows survive.
CREATE VIEW view_result_filter_05107 AS SELECT id, ts FROM dist_05107 SETTINGS additional_result_filter = 'id > 50';

SELECT 'view SETTINGS additional_result_filter disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM view_result_filter_05107 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- `additional_table_filters` filters the table the stored query reads.
CREATE VIEW view_table_filter_05107 AS SELECT id, ts FROM dist_05107 SETTINGS additional_table_filters = {'dist_05107': 'id > 50'};

SELECT 'view SETTINGS additional_table_filters disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM view_table_filter_05107 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- `final` collapses row versions of every table the stored query reads.
CREATE VIEW view_final_05107 AS SELECT id, ts FROM dist_05107 SETTINGS final = 1;

SELECT 'view SETTINGS final disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM view_final_05107 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- A limit with a non-throwing overflow mode returns an arbitrary prefix of the rows read.
CREATE VIEW view_read_limit_05107 AS
SELECT id, ts FROM dist_05107 SETTINGS max_rows_to_read = 10, read_overflow_mode = 'break';

SELECT 'view SETTINGS max_rows_to_read with break disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM view_read_limit_05107 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- Resetting a setting to its default is not tuning either: it can undo a limit the definer's
-- profile sets, so it fails closed like any other non-allowlisted change. (`limit = DEFAULT` would
-- be the direct example, but `InterpreterCreateQuery` rejects query-construction settings in a
-- `VIEW` definition, so reset a setting a definer profile may equally well carry.)
CREATE VIEW view_default_05107 AS SELECT id, ts FROM dist_05107 SETTINGS max_execution_time = DEFAULT;

SELECT 'view SETTINGS reset to DEFAULT disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM view_default_05107 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

DROP VIEW view_plain_05107;
DROP VIEW view_tuning_05107;
DROP VIEW view_result_filter_05107;
DROP VIEW view_table_filter_05107;
DROP VIEW view_final_05107;
DROP VIEW view_read_limit_05107;
DROP VIEW view_default_05107;
DROP TABLE dist_05107;
DROP TABLE local_05107;
