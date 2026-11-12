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
