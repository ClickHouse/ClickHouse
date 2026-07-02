-- Test: exercises `getLeastSupertype` paths for mixing `Enum` with `String`/`FixedString`
-- Covers: src/DataTypes/getLeastSupertype.cpp:691-702 — the
-- `have_string + have_fixed_string + have_enums == type_ids.size()` branch
-- for combinations not covered by the PR's own test
-- (PR test only covers `Array(Enum8 + String + Enum8)`).

-- 1. FixedString + Enum8 in array (no plain String).
SELECT toTypeName([toFixedString('Hello', 5), 'World'::Enum8('World' = 1, 'Goodbye' = 2)]);

-- 2. Enum16 + String in array.
SELECT toTypeName([1::Enum16('Hello' = 1, 'World' = 2), 'Goodbye']);

-- 3. FixedString + Enum16 in array (no plain String).
SELECT toTypeName([toFixedString('abc', 3), 'X'::Enum16('X' = 1, 'Y' = 2)]);

-- 4. Conditional expression: `if(cond, Enum, String)` returns `String`.
SELECT if(0, 'a'::Enum8('a' = 1, 'b' = 2), 'world') AS v, toTypeName(v);

-- 5. Conditional expression: `if(cond, FixedString, Enum)` returns `String`.
SELECT if(1, toFixedString('XX', 2), 'b'::Enum8('a' = 1, 'b' = 2)) AS v, toTypeName(v);

-- 6. UNION ALL of `Enum8` and `String` columns.
SELECT toTypeName(c) FROM (SELECT 'a'::Enum8('a' = 1, 'b' = 2) AS c UNION ALL SELECT 'world'::String) ORDER BY 1 LIMIT 1;

-- 7. Values from a table: `Enum` column unioned with `String` column.
DROP TABLE IF EXISTS test_enum_string_supertype_t1;
DROP TABLE IF EXISTS test_enum_string_supertype_t2;
CREATE TABLE test_enum_string_supertype_t1 (e Enum8('Hello' = 1, 'Goodbye' = 2)) ENGINE = Memory;
CREATE TABLE test_enum_string_supertype_t2 (s String) ENGINE = Memory;
INSERT INTO test_enum_string_supertype_t1 VALUES ('Hello');
INSERT INTO test_enum_string_supertype_t2 VALUES ('World');
SELECT v FROM (SELECT e AS v FROM test_enum_string_supertype_t1 UNION ALL SELECT s AS v FROM test_enum_string_supertype_t2) ORDER BY v;
DROP TABLE test_enum_string_supertype_t1;
DROP TABLE test_enum_string_supertype_t2;
