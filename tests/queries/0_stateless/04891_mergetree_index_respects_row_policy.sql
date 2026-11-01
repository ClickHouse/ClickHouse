-- A row policy on the source table must not be bypassed by the `mergeTreeIndex` table function.

DROP TABLE IF EXISTS t_mt_index_rp;
DROP ROW POLICY IF EXISTS rp_mt_index ON t_mt_index_rp;

CREATE TABLE t_mt_index_rp (id UInt64, department String)
ENGINE = MergeTree ORDER BY (department, id) SETTINGS index_granularity = 1;

INSERT INTO t_mt_index_rp VALUES (1, 'engineering'), (2, 'finance'), (3, 'engineering'), (4, 'hr');

SELECT '-- without a row policy the index is readable';
SELECT DISTINCT department FROM mergeTreeIndex(currentDatabase(), 't_mt_index_rp') ORDER BY department;

CREATE ROW POLICY rp_mt_index ON t_mt_index_rp FOR SELECT USING department = 'engineering' TO ALL;

SELECT '-- base table honours the policy';
SELECT id, department FROM t_mt_index_rp ORDER BY id;

SELECT '-- mergeTreeIndex must not expose primary key values of policy-hidden rows';
SELECT DISTINCT department FROM mergeTreeIndex(currentDatabase(), 't_mt_index_rp') ORDER BY department; -- { serverError ACCESS_DENIED }
SELECT count() FROM mergeTreeIndex(currentDatabase(), 't_mt_index_rp', with_minmax = true); -- { serverError ACCESS_DENIED }

SELECT '-- mergeTreeAnalyzeIndexes returns mark ranges, not values, so the policy does not apply';
SELECT count() >= 0 FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_mt_index_rp, department = 'finance');

DROP ROW POLICY rp_mt_index ON t_mt_index_rp;
DROP TABLE t_mt_index_rp;
