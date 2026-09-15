-- A column matcher inside `additional_table_filters`, `additional_result_filter` or a row policy is
-- resolved against the table the filter applies to, just like the same predicate written in `WHERE`:
-- such an expression is analyzed against a single table expression rather than a query, and matcher
-- resolution used to reject that scope with `There are no table sources`.

DROP TABLE IF EXISTS t_05194;
CREATE TABLE t_05194 (a UInt32, b UInt32, c UInt32 ALIAS a + b) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05194 SELECT number, number FROM numbers(100);

SELECT 'WHERE', count() FROM t_05194 WHERE not ignore(*);
SELECT 'unqualified', count() FROM t_05194 WHERE 1 SETTINGS additional_table_filters = {'t_05194': 'not ignore(*)'};
SELECT 'COLUMNS', count() FROM t_05194 WHERE 1 SETTINGS additional_table_filters = {'t_05194': 'not ignore(COLUMNS(\'.*\'))'};
SELECT 'qualified', count() FROM t_05194 WHERE 1 SETTINGS additional_table_filters = {'t_05194': 'not ignore(t_05194.*)'};
SELECT 'transformer', count() FROM t_05194 WHERE 1 SETTINGS additional_table_filters = {'t_05194': 'not ignore(* EXCEPT b)'};
SELECT 'alias column', count() FROM t_05194 WHERE 1 SETTINGS additional_table_filters = {'t_05194': 'not ignore(*)'}, asterisk_include_alias_columns = 1;

-- The filter is applied, not merely accepted.
SELECT 'filtering', count() FROM t_05194 WHERE 1 SETTINGS additional_table_filters = {'t_05194': 'COLUMNS(\'b\') > 5'};
SELECT 'filtering by an alias column', count() FROM t_05194 WHERE 1 SETTINGS additional_table_filters = {'t_05194': 'COLUMNS(\'c\') > 10'};

-- `additional_result_filter` is applied on top of the whole plan, so the matcher expands the columns
-- of the query result, and the filter sees the rows the `LIMIT` has already selected. Only a single
-- column is printed: `additional_result_filter` also rearranges the result header, which has nothing
-- to do with the matcher, so the two-column case is checked for resolution only.
SELECT 'result filter';
SELECT a FROM t_05194 ORDER BY a LIMIT 3 SETTINGS additional_result_filter = 'not ignore(*)';
SELECT a, b FROM t_05194 ORDER BY a LIMIT 3 SETTINGS additional_result_filter = 'not ignore(*)' FORMAT Null;
SELECT 'result filter, filtering';
SELECT b FROM t_05194 ORDER BY a LIMIT 3 SETTINGS additional_result_filter = 'COLUMNS(\'b\') > 1';

CREATE ROW POLICY OR REPLACE p_05194 ON t_05194 USING not ignore(*) TO ALL;
SELECT 'row policy', count() FROM t_05194;
CREATE ROW POLICY OR REPLACE p_05194 ON t_05194 USING COLUMNS('b') > 5 TO ALL;
SELECT 'row policy, filtering', count() FROM t_05194;
DROP ROW POLICY p_05194 ON t_05194;

-- A matcher is still rejected where the expression must be a constant, which
-- `05051_values_format_names_the_missing_delimiter` pins for a `VALUES` field.

DROP TABLE t_05194;
