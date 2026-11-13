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

DROP TABLE tab_folded_constant;
