-- The `IN` subquery of a standalone expression - a row policy, a `TTL` - is planned by the analyzer as a query
-- of its own. A `MATERIALIZED` CTE in it is materialized with the plan of the set, and a table function in it
-- does not take its structure from the table of an enclosing `INSERT`.

DROP TABLE IF EXISTS in_sub_cte_keys;
DROP TABLE IF EXISTS in_sub_cte_child;
DROP TABLE IF EXISTS in_sub_cte_merge;
DROP TABLE IF EXISTS in_sub_cte_ttl;
DROP TABLE IF EXISTS in_sub_fmt_child;
DROP TABLE IF EXISTS in_sub_fmt_merge;
DROP TABLE IF EXISTS in_sub_fmt_dst;

SET enable_materialized_cte = 1;
-- The `TTL` subquery is analysed in the context of the table, which does not see this setting, so the analyzer warns
-- that `MATERIALIZED` is ignored there.
SET send_logs_level = 'error';

CREATE TABLE in_sub_cte_keys (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO in_sub_cte_keys VALUES (1), (2);

SELECT '-- row policy with a MATERIALIZED CTE, applied while a `Merge` table reads the table';
CREATE TABLE in_sub_cte_child (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO in_sub_cte_child VALUES (1), (2), (3);
CREATE ROW POLICY OR REPLACE in_sub_cte_policy_05290 ON in_sub_cte_child
    USING x IN (WITH t AS MATERIALIZED (SELECT x FROM in_sub_cte_keys) SELECT x FROM t) TO ALL;
CREATE TABLE in_sub_cte_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^in_sub_cte_child$');
SELECT x FROM in_sub_cte_merge ORDER BY x;
DROP ROW POLICY in_sub_cte_policy_05290 ON in_sub_cte_child;

SELECT '-- TTL WHERE with a MATERIALIZED CTE, applied by a merge';
-- The subquery of a `TTL` is resolved outside of the current database, so the table is qualified.
CREATE TABLE in_sub_cte_ttl (x UInt64, d DateTime)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 SECOND WHERE x IN (WITH t AS MATERIALIZED (SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.in_sub_cte_keys) SELECT x FROM t);
INSERT INTO in_sub_cte_ttl VALUES (1, now() - 100), (2, now() - 100), (3, now() - 100);
OPTIMIZE TABLE in_sub_cte_ttl FINAL;
SELECT x FROM in_sub_cte_ttl ORDER BY x;

SELECT '-- row policy with a table function, applied inside INSERT ... SELECT into a table of another structure';
CREATE TABLE in_sub_fmt_child (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO in_sub_fmt_child VALUES (1), (2), (3);
CREATE ROW POLICY OR REPLACE in_sub_fmt_policy_05290 ON in_sub_fmt_child
    USING x IN (SELECT * FROM format(CSV, '1\n2')) TO ALL;
CREATE TABLE in_sub_fmt_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^in_sub_fmt_child$');
CREATE TABLE in_sub_fmt_dst (y String, x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO in_sub_fmt_dst SELECT 'a', x FROM in_sub_fmt_merge;
SELECT y, x FROM in_sub_fmt_dst ORDER BY x;
DROP ROW POLICY in_sub_fmt_policy_05290 ON in_sub_fmt_child;

SELECT '-- the same, with the subquery re-enabling the insertion table structure in its own SETTINGS';
TRUNCATE TABLE in_sub_fmt_dst;
CREATE ROW POLICY OR REPLACE in_sub_fmt_policy_05290 ON in_sub_fmt_child
    USING x IN (SELECT * FROM format(CSV, '1\n2') SETTINGS use_structure_from_insertion_table_in_table_functions = 1) TO ALL;
INSERT INTO in_sub_fmt_dst SELECT 'a', x FROM in_sub_fmt_merge;
SELECT y, x FROM in_sub_fmt_dst ORDER BY x;
DROP ROW POLICY in_sub_fmt_policy_05290 ON in_sub_fmt_child;

DROP TABLE in_sub_fmt_dst;
DROP TABLE in_sub_fmt_merge;
DROP TABLE in_sub_fmt_child;
DROP TABLE in_sub_cte_ttl;
DROP TABLE in_sub_cte_merge;
DROP TABLE in_sub_cte_child;
DROP TABLE in_sub_cte_keys;
