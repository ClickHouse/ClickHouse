-- A column matcher (`*`, `t.*`, `COLUMNS(...)`) in a row policy, `additional_table_filters` or
-- `additional_result_filter` is rejected with a clear diagnostic: a filter is a predicate over the
-- rows of one table, and a matcher there could only ever expand into the arguments of a function
-- such as `ignore(*)`. The analyzer used to fail on it with an obscure `There are no table sources`.

DROP TABLE IF EXISTS t_05227;
DROP TABLE IF EXISTS allowed_05227;
CREATE TABLE t_05227 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05227 SELECT number, number FROM numbers(100);
CREATE TABLE allowed_05227 (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO allowed_05227 SELECT number FROM numbers(10);

SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ignore(*)'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ignore(COLUMNS(\'.*\'))'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ignore(t_05227.*)'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ignore(* EXCEPT b)'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'arrayMap(x -> ignore(*), [1])[1] = 0'}; -- { serverError BAD_ARGUMENTS }
SELECT a FROM t_05227 ORDER BY a LIMIT 3 SETTINGS additional_result_filter = 'not ignore(*)'; -- { serverError BAD_ARGUMENTS }

-- A matcher inside a subquery of the filter resolves against the subquery's own table and is fine.
SELECT 'subquery in a filter', count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'a IN (SELECT * FROM allowed_05227)'};

CREATE ROW POLICY OR REPLACE p_05227 ON t_05227 USING not ignore(*) TO ALL;
SELECT count() FROM t_05227; -- { serverError BAD_ARGUMENTS }

CREATE ROW POLICY OR REPLACE p_05227 ON t_05227 USING a IN (SELECT * FROM allowed_05227) TO ALL;
SELECT 'subquery in a row policy', count() FROM t_05227;
DROP ROW POLICY p_05227 ON t_05227;

-- A matcher-free filter keeps working.
SELECT 'plain filter', count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'b > 5'};
-- `additional_result_filter` is applied on top of the rows the `LIMIT` has already selected.
SELECT 'plain result filter';
SELECT a FROM t_05227 ORDER BY a LIMIT 3 SETTINGS additional_result_filter = 'a > 0';

DROP TABLE t_05227;
DROP TABLE allowed_05227;
