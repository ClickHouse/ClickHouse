-- Tags: no-parallel-replicas
-- The query plan folds constants, so the text index on `concat(tag, toString(1))` must still be found by the original expression
-- to take its tokenizer: `array` here, not `splitByNonAlpha`.

DROP TABLE IF EXISTS tab_folded_constant;
CREATE TABLE tab_folded_constant (id UInt64, tag String, INDEX idx(concat(tag, toString(1))) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, 'env:prod-eu' FROM numbers(64);

SELECT 'hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, toString(1)), 'env:prod') SETTINGS use_skip_indexes = 1;
SELECT 'hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, toString(1)), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenLike', count() FROM tab_folded_constant WHERE hasTokenLike(concat(tag, toString(1)), 'env:prod%') SETTINGS use_skip_indexes = 1;
SELECT 'hasTokenLike, no index', count() FROM tab_folded_constant WHERE hasTokenLike(concat(tag, toString(1)), 'env:prod%') SETTINGS use_skip_indexes = 0;
SELECT 'hasTokenMatch', count() FROM tab_folded_constant WHERE hasTokenMatch(concat(tag, toString(1)), '^env:prod-eu1$') SETTINGS use_skip_indexes = 1;
SELECT 'hasTokenMatch, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(concat(tag, toString(1)), '^env:prod-eu1$') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

-- A numeric constant is folded too: the query names `substring(tag, 1 + 1)` as `substring(tag, 2)`.
CREATE TABLE tab_folded_constant (id UInt64, tag String, INDEX idx(substring(tag, 1 + 1)) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, 'xenv:prod-eu' FROM numbers(64);

SELECT 'numeric hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(substring(tag, 1 + 1), 'env:prod') SETTINGS use_skip_indexes = 1;
SELECT 'numeric hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(substring(tag, 1 + 1), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'numeric hasTokenPrefix, array', count() FROM tab_folded_constant WHERE hasTokenPrefix(substring(tag, 1 + 1), 'env:prod', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'numeric hasAnyTokens', count() FROM tab_folded_constant WHERE hasAnyTokens(substring(tag, 1 + 1), ['env:prod-eu']) SETTINGS use_skip_indexes = 1;
SELECT 'numeric hasAnyTokens, no index', count() FROM tab_folded_constant WHERE hasAnyTokens(substring(tag, 1 + 1), ['env:prod-eu']) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

-- A call that is not folded, like `materialize`, is not evaluated, so inserts and merges still work.
CREATE TABLE tab_folded_constant (id UInt64, tag String, INDEX idx(concat(tag, materialize('1'))) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, 'env:prod-eu' FROM numbers(64);

OPTIMIZE TABLE tab_folded_constant FINAL;
SELECT 'not folded after merge', count() FROM tab_folded_constant;
SELECT 'materialize hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, materialize('1')), 'env:prod') SETTINGS use_skip_indexes = 1;
SELECT 'materialize hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, materialize('1')), 'env:prod') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

-- Constants are folded inside a lambda body too.
CREATE TABLE tab_folded_constant (id UInt64, tags Array(String), INDEX idx(arrayMap(x -> concat(x, toString(1)), tags)) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, ['env:prod-eu'] FROM numbers(64);

SELECT 'lambda hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, toString(1)), tags), 'env:prod') SETTINGS use_skip_indexes = 1;
SELECT 'lambda hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, toString(1)), tags), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'lambda hasTokenLike', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, toString(1)), tags), 'env:prod%') SETTINGS use_skip_indexes = 1;
SELECT 'lambda hasTokenLike, no index', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, toString(1)), tags), 'env:prod%') SETTINGS use_skip_indexes = 0;
SELECT 'lambda hasTokenMatch', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, toString(1)), tags), '^env:prod-eu1$') SETTINGS use_skip_indexes = 1;
SELECT 'lambda hasTokenMatch, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, toString(1)), tags), '^env:prod-eu1$') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

-- A lambda without captured columns is read through the index without the direct read rewrite.
CREATE TABLE tab_folded_constant (id UInt64, tags Array(String), INDEX idx(arrayMap(x -> lower(x), tags)) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, ['ENV:PROD-EU'] FROM numbers(64);

SELECT 'lower lambda hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> lower(x), tags), 'env:prod', 'array');
SELECT 'lower lambda hasTokenPrefix, no direct read', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> lower(x), tags), 'env:prod', 'array') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'lower lambda hasTokenLike', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> lower(x), tags), 'env:prod%');
SELECT 'lower lambda hasTokenMatch', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> lower(x), tags), '^env:prod');
SELECT 'lower lambda hasTokenMatch, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> lower(x), tags), '^env:prod') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;
