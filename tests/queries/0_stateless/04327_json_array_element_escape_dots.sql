SET json_type_escape_dots_in_keys = 1;
SET enable_analyzer = 1;

-- ============================================
-- Setup: keys with dots stored with escape
-- ============================================
DROP TABLE IF EXISTS test_json_dots;
CREATE TABLE test_json_dots (json JSON) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_json_dots FORMAT JSONAsObject {"a.b": 1, "c": {"d.e": 2, "f": 3}};

INSERT INTO test_json_dots FORMAT JSONAsObject {"a.b": 10, "c": {"d.e": 20, "f": 30}, "g.h.i": "hello"};

-- ============================================
-- 1. Single key with dot via bracket syntax
-- ============================================
SELECT 'single_dotted_key';
SELECT json['a.b'] FROM test_json_dots ORDER BY toString(json['a.b']);

-- ============================================
-- 2. Nested access: first key has no dot, second key has dot
-- ============================================
SELECT 'nested_dotted_key';
SELECT json['c']['d.e'] FROM test_json_dots ORDER BY toString(json['c']['d.e']);

-- ============================================
-- 3. Key with multiple dots
-- ============================================
SELECT 'multi_dot_key';
SELECT json['g.h.i'] FROM test_json_dots ORDER BY toString(json['g.h.i']);

-- ============================================
-- 4. Non-dotted key still works
-- ============================================
SELECT 'non_dotted_key';
SELECT json['c']['f'] FROM test_json_dots ORDER BY toString(json['c']['f']);

-- ============================================
-- 5. Equivalence: bracket vs backtick-dot notation
-- ============================================
SELECT 'equivalence_bracket_vs_backtick';
SELECT
    json['a.b'] AS bracket, json.`a%2Eb` AS backtick,
    bracket = backtick AS eq
FROM test_json_dots ORDER BY toString(bracket);

-- ============================================
-- 6. Typed path with dotted key
-- ============================================
DROP TABLE IF EXISTS test_json_dots_typed;
CREATE TABLE test_json_dots_typed (json JSON(`a%2Eb` UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_json_dots_typed FORMAT JSONAsObject {"a.b": 1, "c": 100};

INSERT INTO test_json_dots_typed FORMAT JSONAsObject {"a.b": 2, "c": 200};

SELECT 'typed_dotted_key';
SELECT json['a.b'], toTypeName(json['a.b']) FROM test_json_dots_typed ORDER BY json['a.b'];

-- ============================================
-- 7. EXPLAIN: optimization with dotted keys
-- ============================================
SELECT 'explain_dotted_optimized';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['a.b'] FROM test_json_dots
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'explain_dotted_chain_optimized';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['c']['d.e'] FROM test_json_dots
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'explain_dotted_no_optimization';
EXPLAIN QUERY TREE run_passes = 1
SELECT json['a.b'] FROM test_json_dots
SETTINGS optimize_functions_to_subcolumns = 0;

-- ============================================
-- 8. WHERE clause with dotted key
-- ============================================
SELECT 'where_dotted';
SELECT json['a.b'] FROM test_json_dots WHERE json['a.b'] = 10;

-- ============================================
-- 9. Setting disabled: dots in key should be treated as nested path
-- ============================================
DROP TABLE IF EXISTS test_json_no_escape;
CREATE TABLE test_json_no_escape (json JSON) ENGINE = MergeTree ORDER BY tuple();

SET json_type_escape_dots_in_keys = 0;
INSERT INTO test_json_no_escape FORMAT JSONAsObject {"a": {"b": 42}};

SELECT 'no_escape_nested';
-- With setting off, json['a.b'] should NOT find a.b as a single key; it's a literal
-- key lookup for "a.b" which doesn't exist in the data.
SELECT json['a.b'] FROM test_json_no_escape;

-- But json['a']['b'] accesses nested path as before.
SELECT json['a']['b'] FROM test_json_no_escape;

-- ============================================
-- Cleanup
-- ============================================
DROP TABLE test_json_dots;
DROP TABLE test_json_dots_typed;
DROP TABLE test_json_no_escape;
