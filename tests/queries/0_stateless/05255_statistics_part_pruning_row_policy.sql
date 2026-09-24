-- Statistics describe all rows of a part, including the ones a row policy hides. Pruning parts by them
-- would make the number of rows read an oracle over the values of the hidden rows, so statistics
-- pruning is disabled when a row policy applies, whether the policy belongs to the table, to a child
-- of `Merge`, or to a wrapper such as `Alias`.

DROP ROW POLICY IF EXISTS payroll_policy ON payroll;
DROP ROW POLICY IF EXISTS payroll_alias_policy ON payroll_alias;
DROP TABLE IF EXISTS payroll_merge;
DROP TABLE IF EXISTS payroll_alias;
DROP TABLE IF EXISTS payroll;

SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;
SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE payroll (id UInt64, dept String, salary UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic';

-- Visible rows have `salary` below 150, hidden rows have `salary` up to 1999.
INSERT INTO payroll SELECT number, if(number % 2 = 0, 'public', 'exec'), if(number % 2 = 0, 100 + number % 50, 1000 + number) FROM numbers(1000);

CREATE TABLE payroll_merge (id UInt64, dept String, salary UInt64) ENGINE = Merge(currentDatabase(), '^payroll$');
CREATE TABLE payroll_alias ENGINE = Alias('payroll');

-- Without a row policy the part is pruned when the predicate is out of the range of the statistics.
SELECT count() FROM payroll WHERE salary > 1998 SETTINGS log_comment = '05255_1_no_policy_1998';
SELECT count() FROM payroll WHERE salary > 1999 SETTINGS log_comment = '05255_2_no_policy_1999';

CREATE ROW POLICY payroll_policy ON payroll FOR SELECT USING dept = 'public' TO CURRENT_USER;

SELECT count() FROM payroll WHERE salary > 1998 SETTINGS log_comment = '05255_3_table_policy_1998';
SELECT count() FROM payroll WHERE salary > 1999 SETTINGS log_comment = '05255_4_table_policy_1999';

SELECT count() FROM payroll_merge WHERE salary > 1998 SETTINGS log_comment = '05255_5_merge_child_policy_1998';
SELECT count() FROM payroll_merge WHERE salary > 1999 SETTINGS log_comment = '05255_6_merge_child_policy_1999';

DROP ROW POLICY payroll_policy ON payroll;
CREATE ROW POLICY payroll_alias_policy ON payroll_alias FOR SELECT USING dept = 'public' TO CURRENT_USER;

SELECT count() FROM payroll_alias WHERE salary > 1998 SETTINGS log_comment = '05255_7_alias_policy_1998', enable_analyzer = 1;
SELECT count() FROM payroll_alias WHERE salary > 1999 SETTINGS log_comment = '05255_8_alias_policy_1999', enable_analyzer = 1;

SYSTEM FLUSH LOGS query_log;

-- The number of rows read must not depend on the values of the hidden rows.
SELECT log_comment, read_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05255\_%'
ORDER BY log_comment;

DROP ROW POLICY payroll_alias_policy ON payroll_alias;
DROP TABLE payroll_merge;
DROP TABLE payroll_alias;
DROP TABLE payroll;
