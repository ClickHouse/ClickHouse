-- The trivial count of a `Merge` table sums the `totalRows` of its source tables, which do not
-- know about the row policies applied while the child read plans are built.

-- The test harness may randomize this setting, and the whole test is about it.
SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS t_merge_count_memory;
DROP TABLE IF EXISTS t_merge_count_file;
DROP TABLE IF EXISTS t_merge_count_over_memory;
DROP TABLE IF EXISTS t_merge_count_over_file;
DROP ROW POLICY IF EXISTS policy_05136_memory ON t_merge_count_memory;
DROP ROW POLICY IF EXISTS policy_05136_file ON t_merge_count_file;

CREATE TABLE t_merge_count_memory (k UInt64) ENGINE = Memory;
INSERT INTO t_merge_count_memory SELECT number FROM numbers(10);
CREATE TABLE t_merge_count_over_memory (k UInt64) ENGINE = Merge(currentDatabase(), '^t_merge_count_memory$');

SELECT 'no row policy';
SELECT count() FROM t_merge_count_memory;
SELECT count() FROM t_merge_count_over_memory;

CREATE ROW POLICY policy_05136_memory ON t_merge_count_memory FOR SELECT USING k < 3 TO CURRENT_USER;

SELECT 'row policy on the Memory source table';
SELECT count() FROM t_merge_count_memory;
SELECT count() FROM t_merge_count_over_memory;
SELECT count() FROM t_merge_count_over_memory SETTINGS optimize_trivial_count_query = 0;

-- The same for a storage that counts the rows in `read` instead of `totalRows`.
CREATE TABLE t_merge_count_file (k UInt64) ENGINE = File(TSV);
INSERT INTO t_merge_count_file SELECT number FROM numbers(10);
CREATE TABLE t_merge_count_over_file (k UInt64) ENGINE = Merge(currentDatabase(), '^t_merge_count_file$');
CREATE ROW POLICY policy_05136_file ON t_merge_count_file FOR SELECT USING k < 3 TO CURRENT_USER;

SELECT 'row policy on the File source table';
SELECT count() FROM t_merge_count_file;
SELECT count() FROM t_merge_count_over_file;
SELECT count() FROM t_merge_count_over_file SETTINGS optimize_trivial_count_query = 0;

DROP ROW POLICY policy_05136_memory ON t_merge_count_memory;
DROP ROW POLICY policy_05136_file ON t_merge_count_file;
DROP TABLE t_merge_count_over_memory;
DROP TABLE t_merge_count_over_file;
DROP TABLE t_merge_count_memory;
DROP TABLE t_merge_count_file;
