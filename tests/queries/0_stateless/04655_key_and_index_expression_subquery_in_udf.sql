-- Tags: no-parallel
-- - no-parallel -- SQL UDFs are global server objects; the flaky check runs the same test concurrently and the CREATE FUNCTION statements would collide.

-- Key expressions (`ORDER BY`, `PARTITION BY`) and skip-index expressions cannot contain
-- subqueries or column matchers, and hiding them inside a SQL user-defined function must not
-- bypass the ban: `InterpreterCreateQuery` and `InterpreterAlterQuery` inline SQL UDF bodies into
-- the query before the table metadata is built, so the validation sees the expanded expression.

DROP FUNCTION IF EXISTS f_04655_in_set;
DROP FUNCTION IF EXISTS f_04655_scalar;
DROP FUNCTION IF EXISTS f_04655_matcher;
DROP FUNCTION IF EXISTS f_04655_plain;
DROP TABLE IF EXISTS key_udf_src;
DROP TABLE IF EXISTS key_udf_order_by;
DROP TABLE IF EXISTS key_udf_partition_by;
DROP TABLE IF EXISTS key_udf_index;
DROP TABLE IF EXISTS key_udf_matcher;
DROP TABLE IF EXISTS key_udf_alter;
DROP TABLE IF EXISTS key_udf_plain;
DROP TABLE IF EXISTS key_alias_index;

CREATE TABLE key_udf_src (id UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE FUNCTION f_04655_in_set AS x -> x IN (SELECT id FROM key_udf_src);
CREATE FUNCTION f_04655_scalar AS x -> x + (SELECT count() FROM key_udf_src);

-- A subquery hidden in a UDF is rejected in a key expression.
CREATE TABLE key_udf_order_by (x UInt64) ENGINE = MergeTree ORDER BY f_04655_in_set(x); -- { serverError BAD_ARGUMENTS }
CREATE TABLE key_udf_partition_by (x UInt64) ENGINE = MergeTree ORDER BY x PARTITION BY f_04655_scalar(x); -- { serverError BAD_ARGUMENTS }

-- The same for a skip-index expression, both at `CREATE` and at `ALTER` time.
CREATE TABLE key_udf_index (x UInt64, INDEX idx f_04655_in_set(x) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Alias replacement happens after the initial index-definition validation, so revalidate the
-- expanded expression before it is analyzed.
CREATE TABLE key_alias_index (x UInt64, a UInt8 ALIAS x IN key_udf_src, INDEX idx a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE key_udf_alter (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE key_udf_alter ADD INDEX idx f_04655_in_set(y) TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE key_udf_alter MODIFY ORDER BY (x, f_04655_scalar(y)); -- { serverError BAD_ARGUMENTS }

-- A column matcher hidden in a UDF is rejected in a key expression as well: the Analyzer would
-- expand it into the matched columns, desyncing the key column names from the key sample block.
CREATE FUNCTION f_04655_matcher AS x -> (COLUMNS('^a$'), x);
CREATE TABLE key_udf_matcher (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY f_04655_matcher(b); -- { serverError BAD_ARGUMENTS }

-- A UDF without a subquery keeps working in a key expression.
CREATE FUNCTION f_04655_plain AS x -> x * 2;
CREATE TABLE key_udf_plain (x UInt64) ENGINE = MergeTree ORDER BY f_04655_plain(x);
INSERT INTO key_udf_plain VALUES (2), (1);
SELECT x FROM key_udf_plain ORDER BY x;
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'key_udf_plain';

DROP TABLE key_udf_plain;
DROP TABLE key_udf_alter;
DROP TABLE key_udf_src;

DROP FUNCTION f_04655_plain;
DROP FUNCTION f_04655_matcher;
DROP FUNCTION f_04655_scalar;
DROP FUNCTION f_04655_in_set;
