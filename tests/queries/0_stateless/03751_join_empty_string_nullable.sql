-- Test that empty string is not converted to NULL in Join table with Nullable(String) column
-- https://github.com/ClickHouse/ClickHouse/issues/71414

DROP TABLE IF EXISTS t_join_nullable_string;

-- Case 1: Nullable(String) key column with INNER JOIN
CREATE TABLE t_join_nullable_string (c0 Nullable(String)) ENGINE = Join(ALL, INNER, c0);
INSERT INTO TABLE t_join_nullable_string (c0) VALUES ('');
SELECT c0, c0 IS NULL as is_null FROM t_join_nullable_string;
DROP TABLE t_join_nullable_string;

-- Case 2: Nullable(String) key column with LEFT JOIN (NULL keys are not stored in join tables)
CREATE TABLE t_join_nullable_string (c0 Nullable(String)) ENGINE = Join(ALL, LEFT, c0);
INSERT INTO TABLE t_join_nullable_string (c0) VALUES (''), ('abc');
SELECT c0, c0 IS NULL as is_null FROM t_join_nullable_string ORDER BY c0 NULLS LAST;
DROP TABLE t_join_nullable_string;

-- Case 3: ANY strictness
CREATE TABLE t_join_nullable_string (c0 Nullable(String)) ENGINE = Join(ANY, LEFT, c0);
INSERT INTO TABLE t_join_nullable_string (c0) VALUES ('');
SELECT c0, c0 IS NULL as is_null FROM t_join_nullable_string;
DROP TABLE t_join_nullable_string;

-- Case 4: Multiple empty strings (should deduplicate for ANY)
CREATE TABLE t_join_nullable_string (c0 Nullable(String), val UInt64) ENGINE = Join(ANY, LEFT, c0);
INSERT INTO TABLE t_join_nullable_string (c0, val) VALUES ('', 1), ('', 2);
SELECT c0, val FROM t_join_nullable_string;
DROP TABLE t_join_nullable_string;

-- Case 5: Multiple empty strings with ALL strictness (should keep all)
CREATE TABLE t_join_nullable_string (c0 Nullable(String), val UInt64) ENGINE = Join(ALL, LEFT, c0);
INSERT INTO TABLE t_join_nullable_string (c0, val) VALUES ('', 1), ('', 2);
SELECT c0, val FROM t_join_nullable_string ORDER BY val;
DROP TABLE t_join_nullable_string;
