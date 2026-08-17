-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

SET enable_analyzer = 1;
SET optimize_inverse_dictionary_lookup = 1;
-- Keep `LIKE` as `like` in EXPLAIN output regardless of settings randomization.
SET optimize_rewrite_like_perfect_affix = 0;

DROP DICTIONARY IF EXISTS dict_single_key;
DROP DICTIONARY IF EXISTS dict_two_keys;
DROP TABLE IF EXISTS ref_source;
DROP TABLE IF EXISTS data;

CREATE TABLE ref_source
(
    k UUID,
    k2 String,
    attr String
)
ENGINE = MergeTree
ORDER BY k;

INSERT INTO ref_source VALUES
    ('11111111-1111-1111-1111-111111111111', 'a', 'paywall'),
    ('22222222-2222-2222-2222-222222222222', 'b', 'onboarding'),
    ('44444444-4444-4444-4444-444444444444', 'd', 'paywall');

-- Complex-key dictionary with a single key column: `dictGet` accepts both the bare
-- key expression (`dictGet(..., k)`) and its one-element tuple wrapper
-- (`dictGet(..., tuple(k))`).
CREATE DICTIONARY dict_single_key
(
    k UUID,
    attr String DEFAULT 'none'
)
PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'ref_source'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);

CREATE DICTIONARY dict_two_keys
(
    k UUID,
    k2 String,
    attr String DEFAULT ''
)
PRIMARY KEY k, k2
SOURCE(CLICKHOUSE(TABLE 'ref_source'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0);

CREATE TABLE data
(
    k UUID,
    k2 String,
    kt Tuple(UUID)
)
ENGINE = MergeTree
ORDER BY k;

INSERT INTO data VALUES
    ('11111111-1111-1111-1111-111111111111', 'a', tuple('11111111-1111-1111-1111-111111111111')),
    ('22222222-2222-2222-2222-222222222222', 'b', tuple('22222222-2222-2222-2222-222222222222')),
    ('33333333-3333-3333-3333-333333333333', 'c', tuple('33333333-3333-3333-3333-333333333333')),
    ('44444444-4444-4444-4444-444444444444', 'd', tuple('44444444-4444-4444-4444-444444444444'));

-- The degenerate case: single-column complex key called with a tuple() wrapper.
-- `equals` with one matching key constant-folds into `key = const`.
SELECT 'tuple(k), equals, one match - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'onboarding';
SELECT 'tuple(k), equals, one match';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'onboarding';
SELECT 'tuple(k), equals, one match, opt off';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'onboarding'
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- `equals` with several matching keys constant-folds into `key IN [consts]`.
SELECT 'tuple(k), equals, two matches - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'paywall';
SELECT 'tuple(k), equals, two matches';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'paywall';
SELECT 'tuple(k), equals, two matches, opt off';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'paywall'
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- `equals` with no matching keys constant-folds into `0`.
SELECT 'tuple(k), equals, no matches - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'missing';
SELECT 'tuple(k), equals, no matches';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'missing';
SELECT 'tuple(k), equals, no matches, opt off';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) = 'missing'
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- `notEquals` against the attribute default rewrites into
-- `key IN (SELECT key FROM dictionary(...) WHERE ...)`.
SELECT 'tuple(k), notEquals - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) != 'none';
SELECT 'tuple(k), notEquals';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) != 'none';
SELECT 'tuple(k), notEquals, opt off';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) != 'none'
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- `like` also rewrites into the IN-subquery form.
SELECT 'tuple(k), like - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) LIKE 'pay%';
SELECT 'tuple(k), like';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) LIKE 'pay%';
SELECT 'tuple(k), like, opt off';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', tuple(k)) LIKE 'pay%'
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- The key expression can have a one-element tuple type without being a literal
-- `tuple(...)` call: a column of type `Tuple(UUID)`.
SELECT 'tuple-typed column, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', kt) = 'onboarding';
SELECT 'tuple-typed column, equals';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', kt) = 'onboarding';
SELECT 'tuple-typed column, equals, opt off';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', kt) = 'onboarding'
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'tuple-typed column, like - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', kt) LIKE 'pay%';
SELECT 'tuple-typed column, like';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', kt) LIKE 'pay%';
SELECT 'tuple-typed column, like, opt off';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', kt) LIKE 'pay%'
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- The one-element tuple can be Nullable (e.g. produced by `if`): `dictGet` returns
-- NULL for NULL keys, and so does the rewritten comparison.
SELECT 'nullable tuple expr, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data
WHERE dictGet('dict_single_key', 'attr', if(k != '33333333-3333-3333-3333-333333333333', tuple(k), NULL)) = 'onboarding';
SELECT 'nullable tuple expr, equals';
SELECT count() FROM data
WHERE dictGet('dict_single_key', 'attr', if(k != '33333333-3333-3333-3333-333333333333', tuple(k), NULL)) = 'onboarding';
SELECT 'nullable tuple expr, equals, opt off';
SELECT count() FROM data
WHERE dictGet('dict_single_key', 'attr', if(k != '33333333-3333-3333-3333-333333333333', tuple(k), NULL)) = 'onboarding'
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'nullable tuple expr, like - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data
WHERE dictGet('dict_single_key', 'attr', if(k != '33333333-3333-3333-3333-333333333333', tuple(k), NULL)) LIKE 'pay%';
SELECT 'nullable tuple expr, like';
SELECT count() FROM data
WHERE dictGet('dict_single_key', 'attr', if(k != '33333333-3333-3333-3333-333333333333', tuple(k), NULL)) LIKE 'pay%';
SELECT 'nullable tuple expr, like, opt off';
SELECT count() FROM data
WHERE dictGet('dict_single_key', 'attr', if(k != '33333333-3333-3333-3333-333333333333', tuple(k), NULL)) LIKE 'pay%'
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- Control: the bare key form must keep working exactly as before.
SELECT 'bare key, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', k) = 'onboarding';
SELECT 'bare key, equals';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', k) = 'onboarding';
SELECT 'bare key, equals, opt off';
SELECT count() FROM data WHERE dictGet('dict_single_key', 'attr', k) = 'onboarding'
SETTINGS optimize_inverse_dictionary_lookup = 0;

-- Control: the constant can be on the left-hand side.
SELECT 'tuple(k), constant on the left';
SELECT count() FROM data WHERE 'onboarding' = dictGet('dict_single_key', 'attr', tuple(k));

-- Control: a two-column key is the standard form and must stay untouched.
SELECT 'two-column key, equals - plan';
EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count() FROM data WHERE dictGet('dict_two_keys', 'attr', (k, k2)) = 'paywall';
SELECT 'two-column key, equals';
SELECT count() FROM data WHERE dictGet('dict_two_keys', 'attr', (k, k2)) = 'paywall';
SELECT 'two-column key, equals, opt off';
SELECT count() FROM data WHERE dictGet('dict_two_keys', 'attr', (k, k2)) = 'paywall'
SETTINGS optimize_inverse_dictionary_lookup = 0;
