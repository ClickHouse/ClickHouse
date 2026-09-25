-- Runtime behavior of mapContainsKeyValue(map, key, value) and
-- mapContainsKeyValueLike(map, key_pattern, value_pattern) on plain Map columns (no index).
-- mapContainsKeyValue is the correlated-pair analogue of mapContainsKey / mapContainsValue: it is true
-- iff the map has at least one entry whose key equals `key` AND value equals `value`
-- (semantically has(map::Array(Tuple(K, V)), (key, value))). The Like variant matches each side by LIKE.

-- ============================================================================
-- mapContainsKeyValue: constants
-- ============================================================================
SELECT mapContainsKeyValue(map('a', '1', 'b', '2'), 'a', '1');   -- 1
SELECT mapContainsKeyValue(map('a', '1', 'b', '2'), 'b', '2');   -- 1
SELECT mapContainsKeyValue(map('a', '1', 'b', '2'), 'a', '2');   -- 0 (key present, value wrong)
SELECT mapContainsKeyValue(map('a', '1', 'b', '2'), 'c', '1');   -- 0 (key absent)
SELECT mapContainsKeyValue(map('a', '1'), 'a', '');              -- 0 (empty needle value)
SELECT mapContainsKeyValue(map(), 'a', '1');                     -- 0 (empty map)

-- Duplicate keys: matches any occurrence (unlike m['k'] which returns the first value).
SELECT mapContainsKeyValue(map('k', 'a', 'k', 'b'), 'k', 'a');   -- 1
SELECT mapContainsKeyValue(map('k', 'a', 'k', 'b'), 'k', 'b');   -- 1
SELECT mapContainsKeyValue(map('k', 'a', 'k', 'b'), 'k', 'c');   -- 0

-- Non-String key/value types (delegates type checking to has via getLeastSupertype).
SELECT mapContainsKeyValue(map('a', 1, 'b', 2), 'a', 1);         -- 1  Map(String, UInt8)
SELECT mapContainsKeyValue(map('a', 1, 'b', 2), 'a', 2);         -- 0
SELECT mapContainsKeyValue(map(1, 'x', 2, 'y'), 2, 'y');         -- 1  Map(UInt8, String)
SELECT mapContainsKeyValue(map('a', 1), 'a', 1000);              -- 0  (numeric supertype, no false match)

-- First argument must be a Map.
SELECT mapContainsKeyValue([1, 2], 1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Nullable value type: composing the two comparisons with `and` handles NULL (no exception); a NULL
-- value never matches (three-valued logic collapses to not-a-match under the existential mapContains).
SELECT mapContainsKeyValue(map('a', toNullable('x'), 'b', CAST(NULL AS Nullable(String))), 'a', 'x');   -- 1
SELECT mapContainsKeyValue(map('a', toNullable('x'), 'b', CAST(NULL AS Nullable(String))), 'b', 'x');   -- 0 (b's value is NULL)
SELECT mapContainsKeyValue(map('a', CAST(NULL AS Nullable(String))), 'a', 'x');                         -- 0 (NULL value, not a match)

-- ============================================================================
-- mapContainsKeyValueLike: constants (LIKE on both key and value)
-- ============================================================================
SELECT mapContainsKeyValueLike(map('level', 'error', 'svc', 'api'), 'lev%', '%rror%');  -- 1
SELECT mapContainsKeyValueLike(map('level', 'error'), 'level', 'error');                -- 1 (no wildcards = exact)
SELECT mapContainsKeyValueLike(map('level', 'error'), 'lev%', 'info%');                 -- 0 (value pattern fails)
SELECT mapContainsKeyValueLike(map('level', 'error'), 'svc%', '%rror%');                -- 0 (key pattern fails)
SELECT mapContainsKeyValueLike(map('level', 'error'), 'l_v_l', 'err_r');                -- 1 (underscore wildcard)
SELECT mapContainsKeyValueLike(map('a', 'b'), '%', '%');                                -- 1 (matches any single entry)
SELECT mapContainsKeyValueLike(map()::Map(String, String), 'a%', 'b%');                 -- 0 (empty map; typed, since the Like variant needs a String key type)
-- Duplicate keys: any matching pair.
SELECT mapContainsKeyValueLike(map('k', 'apple', 'k', 'banana'), 'k', 'ban%');          -- 1

-- ============================================================================
-- Over table columns (const and non-const map, key, value).
-- ============================================================================
DROP TABLE IF EXISTS t_mckv;
CREATE TABLE t_mckv (id UInt64, m Map(String, String), k String, v String) ENGINE = Memory;
INSERT INTO t_mckv VALUES
    (1, map('level', 'error', 'svc', 'api'), 'level', 'error'),
    (2, map('level', 'info'),                'level', 'error'),
    (3, map('svc', 'api', 'env', 'prod'),    'svc',   'api'),
    (4, map('k', 'a', 'k', 'b'),             'k',     'b'),
    (5, map(),                               'x',     'y');

SELECT '-- column map, column key, column value --';
SELECT id, mapContainsKeyValue(m, k, v) FROM t_mckv ORDER BY id;
SELECT '-- column map, const key, const value --';
SELECT id FROM t_mckv WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT '-- Like over column map --';
SELECT id FROM t_mckv WHERE mapContainsKeyValueLike(m, '%', 'a%') ORDER BY id;

DROP TABLE t_mckv;

-- ============================================================================
-- LowCardinality keys/values.
-- ============================================================================
DROP TABLE IF EXISTS t_mckv_lc;
CREATE TABLE t_mckv_lc (id UInt64, m Map(LowCardinality(String), LowCardinality(String))) ENGINE = Memory;
INSERT INTO t_mckv_lc VALUES (1, map('level', 'error')), (2, map('level', 'info', 'svc', 'api'));
SELECT id FROM t_mckv_lc WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT id FROM t_mckv_lc WHERE mapContainsKeyValueLike(m, 'svc', 'ap%') ORDER BY id;
DROP TABLE t_mckv_lc;
