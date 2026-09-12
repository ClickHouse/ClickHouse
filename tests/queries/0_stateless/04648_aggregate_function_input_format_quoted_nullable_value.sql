-- The quoted `VALUES` form of `aggregate_function_input_format = 'value'` was released parsing the content
-- of the quoted token with the argument type's `deserializeTextCSV`, so the CSV null representation `\N`
-- built a null state for a single `Nullable` argument. Check that this form still works.

SET aggregate_function_input_format = 'value';

DROP TABLE IF EXISTS t_quoted_nullable_value_str;
CREATE TABLE t_quoted_nullable_value_str (x AggregateFunction(any, Nullable(String))) ENGINE = Memory;

-- The released CSV null representation.
INSERT INTO t_quoted_nullable_value_str VALUES ('\\N');
SELECT 'string backslash N', anyMerge(x) IS NULL FROM t_quoted_nullable_value_str;

-- `NULL` and `null` stay strings for a string-like nested type, as released.
TRUNCATE TABLE t_quoted_nullable_value_str;
INSERT INTO t_quoted_nullable_value_str VALUES ('NULL'), ('null'), ('abc');
SELECT 'string values', anyMerge(x) IS NULL, anyMerge(x) FROM t_quoted_nullable_value_str GROUP BY x ORDER BY anyMerge(x);

-- The native unquoted `NULL` keyword still builds a null state.
TRUNCATE TABLE t_quoted_nullable_value_str;
INSERT INTO t_quoted_nullable_value_str VALUES (NULL);
SELECT 'string native NULL', anyMerge(x) IS NULL FROM t_quoted_nullable_value_str;

DROP TABLE t_quoted_nullable_value_str;

DROP TABLE IF EXISTS t_quoted_nullable_value_num;
CREATE TABLE t_quoted_nullable_value_num (x AggregateFunction(any, Nullable(UInt64))) ENGINE = Memory;

INSERT INTO t_quoted_nullable_value_num VALUES ('\\N');
SELECT 'number backslash N', anyMerge(x) IS NULL FROM t_quoted_nullable_value_num;

TRUNCATE TABLE t_quoted_nullable_value_num;
INSERT INTO t_quoted_nullable_value_num VALUES ('42');
SELECT 'number value', anyMerge(x) IS NULL, anyMerge(x) FROM t_quoted_nullable_value_num;

TRUNCATE TABLE t_quoted_nullable_value_num;
INSERT INTO t_quoted_nullable_value_num VALUES (NULL);
SELECT 'number native NULL', anyMerge(x) IS NULL FROM t_quoted_nullable_value_num;

DROP TABLE t_quoted_nullable_value_num;

-- `LowCardinality(Nullable(...))` takes the same path.
DROP TABLE IF EXISTS t_quoted_nullable_value_lc;
CREATE TABLE t_quoted_nullable_value_lc (x AggregateFunction(any, LowCardinality(Nullable(String)))) ENGINE = Memory;

INSERT INTO t_quoted_nullable_value_lc VALUES ('\\N');
SELECT 'low cardinality backslash N', anyMerge(x) IS NULL FROM t_quoted_nullable_value_lc;

TRUNCATE TABLE t_quoted_nullable_value_lc;
INSERT INTO t_quoted_nullable_value_lc VALUES ('abc');
SELECT 'low cardinality value', anyMerge(x) IS NULL, anyMerge(x) FROM t_quoted_nullable_value_lc;

DROP TABLE t_quoted_nullable_value_lc;

-- A non-`Nullable` argument is not affected.
DROP TABLE IF EXISTS t_quoted_value_plain;
CREATE TABLE t_quoted_value_plain (x AggregateFunction(any, String)) ENGINE = Memory;
INSERT INTO t_quoted_value_plain VALUES ('abc');
SELECT 'plain value', anyMerge(x) FROM t_quoted_value_plain;
DROP TABLE t_quoted_value_plain;
