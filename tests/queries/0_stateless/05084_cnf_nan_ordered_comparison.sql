-- `NaN` fails every ordered comparison, so `NOT (x < c)` is true for a `NaN` while `x >= c` is false.
-- The CNF converter inverted one into the other, which silently dropped the `NaN` rows.

DROP TABLE IF EXISTS t_cnf_nan;
CREATE TABLE t_cnf_nan (x Float64, i Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cnf_nan VALUES (nan, 1), (1, 2), (100, 3);

SELECT 'not less';
SELECT count() FROM t_cnf_nan WHERE NOT (x < 65.5) SETTINGS convert_query_to_cnf = 0;
SELECT count() FROM t_cnf_nan WHERE NOT (x < 65.5) SETTINGS convert_query_to_cnf = 1;
SELECT count() FROM t_cnf_nan WHERE NOT (x < 65.5) SETTINGS convert_query_to_cnf = 1, enable_analyzer = 0;

SELECT 'not greater';
SELECT count() FROM t_cnf_nan WHERE NOT (x > 65.5) SETTINGS convert_query_to_cnf = 0;
SELECT count() FROM t_cnf_nan WHERE NOT (x > 65.5) SETTINGS convert_query_to_cnf = 1;
SELECT count() FROM t_cnf_nan WHERE NOT (x > 65.5) SETTINGS convert_query_to_cnf = 1, enable_analyzer = 0;

SELECT 'not less or equals';
SELECT count() FROM t_cnf_nan WHERE NOT (x <= 65.5) SETTINGS convert_query_to_cnf = 0;
SELECT count() FROM t_cnf_nan WHERE NOT (x <= 65.5) SETTINGS convert_query_to_cnf = 1;

SELECT 'not greater or equals';
SELECT count() FROM t_cnf_nan WHERE NOT (x >= 65.5) SETTINGS convert_query_to_cnf = 0;
SELECT count() FROM t_cnf_nan WHERE NOT (x >= 65.5) SETTINGS convert_query_to_cnf = 1;

-- The equality operators are complementary for a `NaN` as well, so they keep being inverted.
SELECT 'not equals';
SELECT count() FROM t_cnf_nan WHERE NOT (x = 1) SETTINGS convert_query_to_cnf = 0;
SELECT count() FROM t_cnf_nan WHERE NOT (x = 1) SETTINGS convert_query_to_cnf = 1;
SELECT 'not in';
SELECT count() FROM t_cnf_nan WHERE NOT (x IN (1, 100)) SETTINGS convert_query_to_cnf = 0;
SELECT count() FROM t_cnf_nan WHERE NOT (x IN (1, 100)) SETTINGS convert_query_to_cnf = 1;

-- An integer column cannot hold a `NaN`, so its comparison is still inverted into the CNF form.
SELECT 'the integer comparison is still inverted';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_cnf_nan WHERE NOT (i < 2) SETTINGS convert_query_to_cnf = 1)
WHERE explain LIKE '%function_name: greaterOrEquals%';
SELECT count() FROM t_cnf_nan WHERE NOT (i < 2) SETTINGS convert_query_to_cnf = 1;
SELECT count() FROM t_cnf_nan WHERE NOT (i < 2) SETTINGS convert_query_to_cnf = 0;

-- A float comparison is not, even though the constant itself is not a `NaN`.
SELECT 'the float comparison is not';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_cnf_nan WHERE NOT (x < 65.5) SETTINGS convert_query_to_cnf = 1)
WHERE explain LIKE '%function_name: greaterOrEquals%';

DROP TABLE t_cnf_nan;
