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

-- A lambda that captures a folded higher-order expression keeps the index tokenizer.
CREATE TABLE tab_folded_constant (id UInt64, tags Array(String), INDEX idx(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags)) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, ['env:prod-eu'] FROM numbers(64);

SELECT 'arrayFirst capture hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), 'env:prod') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst capture hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst capture hasTokenPrefix array', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), 'env:prod', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst capture hasTokenPrefix array, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), 'env:prod', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst capture hasTokenLike', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), 'env:prod%') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst capture hasTokenLike, no index', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), 'env:prod%') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst capture hasTokenLike array', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), 'env:prod%', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst capture hasTokenLike array, no index', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), 'env:prod%', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst capture hasTokenMatch', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), '^env:prod-eu1$') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst capture hasTokenMatch, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), '^env:prod-eu1$') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst capture hasTokenMatch array', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), '^env:prod-eu1$', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst capture hasTokenMatch array, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, arrayFirst(y -> 1, ['1'])), tags), '^env:prod-eu1$', 'array') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;
CREATE TABLE tab_folded_constant (id UInt64, tags Array(String), INDEX idx(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags)) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, ['env:prod-eu'] FROM numbers(64);

SELECT 'arraySum capture hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), 'env:prod') SETTINGS use_skip_indexes = 1;
SELECT 'arraySum capture hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'arraySum capture hasTokenPrefix array', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), 'env:prod', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arraySum capture hasTokenPrefix array, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), 'env:prod', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'arraySum capture hasTokenLike', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), 'env:prod%') SETTINGS use_skip_indexes = 1;
SELECT 'arraySum capture hasTokenLike, no index', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), 'env:prod%') SETTINGS use_skip_indexes = 0;
SELECT 'arraySum capture hasTokenLike array', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), 'env:prod%', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arraySum capture hasTokenLike array, no index', count() FROM tab_folded_constant WHERE hasTokenLike(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), 'env:prod%', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'arraySum capture hasTokenMatch', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), '^env:prod-eu1$') SETTINGS use_skip_indexes = 1;
SELECT 'arraySum capture hasTokenMatch, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), '^env:prod-eu1$') SETTINGS use_skip_indexes = 0;
SELECT 'arraySum capture hasTokenMatch array', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), '^env:prod-eu1$', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arraySum capture hasTokenMatch array, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(arrayMap(x -> concat(x, toString(arraySum(arrayMap(y -> y + 1, [0])))), tags), '^env:prod-eu1$', 'array') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

CREATE TABLE tab_folded_constant (id UInt64, tag String, INDEX idx(concat(tag, toString(arrayFirst(y -> 1, ['1'])))) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, 'env:prod-eu' FROM numbers(64);

SELECT 'arrayFirst scalar hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, toString(arrayFirst(y -> 1, ['1']))), 'env:prod') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst scalar hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, toString(arrayFirst(y -> 1, ['1']))), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst scalar hasTokenPrefix array', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, toString(arrayFirst(y -> 1, ['1']))), 'env:prod', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst scalar hasTokenPrefix array, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, toString(arrayFirst(y -> 1, ['1']))), 'env:prod', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst scalar hasTokenLike', count() FROM tab_folded_constant WHERE hasTokenLike(concat(tag, toString(arrayFirst(y -> 1, ['1']))), 'env:prod%') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst scalar hasTokenLike, no index', count() FROM tab_folded_constant WHERE hasTokenLike(concat(tag, toString(arrayFirst(y -> 1, ['1']))), 'env:prod%') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst scalar hasTokenLike array', count() FROM tab_folded_constant WHERE hasTokenLike(concat(tag, toString(arrayFirst(y -> 1, ['1']))), 'env:prod%', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst scalar hasTokenLike array, no index', count() FROM tab_folded_constant WHERE hasTokenLike(concat(tag, toString(arrayFirst(y -> 1, ['1']))), 'env:prod%', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst scalar hasTokenMatch', count() FROM tab_folded_constant WHERE hasTokenMatch(concat(tag, toString(arrayFirst(y -> 1, ['1']))), '^env:prod-eu1$') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst scalar hasTokenMatch, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(concat(tag, toString(arrayFirst(y -> 1, ['1']))), '^env:prod-eu1$') SETTINGS use_skip_indexes = 0;
SELECT 'arrayFirst scalar hasTokenMatch array', count() FROM tab_folded_constant WHERE hasTokenMatch(concat(tag, toString(arrayFirst(y -> 1, ['1']))), '^env:prod-eu1$', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'arrayFirst scalar hasTokenMatch array, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(concat(tag, toString(arrayFirst(y -> 1, ['1']))), '^env:prod-eu1$', 'array') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

-- A function that a query does not fold, like `throwIf` or `identity`, is not run when the index expression is named.
CREATE TABLE tab_folded_constant (id UInt64, tag String, INDEX idx(if(id > 0, tag, toString(throwIf(materialize(1))))) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number + 1, 'env:prod-eu' FROM numbers(64);

SELECT 'not folded throwIf', count() FROM tab_folded_constant;

DROP TABLE tab_folded_constant;

CREATE TABLE tab_folded_constant (id UInt64, tag String, INDEX idx(concat(tag, identity(materialize('1')))) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, 'env:prod-eu' FROM numbers(64);

SELECT 'not folded identity hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, identity(materialize('1'))), 'env:prod') SETTINGS use_skip_indexes = 1;
SELECT 'not folded identity hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, identity(materialize('1'))), 'env:prod') SETTINGS use_skip_indexes = 0;
SELECT 'not folded identity hasTokenPrefix array', count() FROM tab_folded_constant WHERE hasTokenPrefix(concat(tag, identity(materialize('1'))), 'env:prod', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'not folded identity hasTokenLike', count() FROM tab_folded_constant WHERE hasTokenLike(concat(tag, identity(materialize('1'))), 'env:prod%') SETTINGS use_skip_indexes = 1;
SELECT 'not folded identity hasTokenLike, no index', count() FROM tab_folded_constant WHERE hasTokenLike(concat(tag, identity(materialize('1'))), 'env:prod%') SETTINGS use_skip_indexes = 0;
SELECT 'not folded identity hasTokenMatch', count() FROM tab_folded_constant WHERE hasTokenMatch(concat(tag, identity(materialize('1'))), '^env:prod-eu1$') SETTINGS use_skip_indexes = 1;
SELECT 'not folded identity hasTokenMatch, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(concat(tag, identity(materialize('1'))), '^env:prod-eu1$') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

-- A `Date` constant is not named by its number: the index is not the one of the expression with the `UInt16` number.
CREATE TABLE tab_folded_constant (id UInt64, tag String, INDEX idx(formatRowNoNewline('CSV', tag, toDate('2026-01-01'))) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, 'env:prod-eu' FROM numbers(64);

SELECT 'typed constant LIKE', count() FROM tab_folded_constant WHERE formatRowNoNewline('CSV', tag, toUInt16(toDate('2026-01-01'))) LIKE '%20454%' SETTINGS use_skip_indexes = 1;
SELECT 'typed constant LIKE, no index', count() FROM tab_folded_constant WHERE formatRowNoNewline('CSV', tag, toUInt16(toDate('2026-01-01'))) LIKE '%20454%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

-- A `Bool` constant is not named by its number either: `toString` of the `Bool` result is `true`, not `1`.
CREATE TABLE tab_folded_constant (id UInt64, c UInt8, INDEX idx(toString(if(c, toBool(1), toBool(0)))) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, 1 FROM numbers(64);

SELECT 'Bool constant hasAnyTokens', count() FROM tab_folded_constant WHERE hasAnyTokens(toString(if(c, 1, 0)), ['1']) SETTINGS use_skip_indexes = 1;
SELECT 'Bool constant hasAnyTokens, no index', count() FROM tab_folded_constant WHERE hasAnyTokens(toString(if(c, 1, 0)), ['1']) SETTINGS use_skip_indexes = 0;
SELECT 'Bool constant hasTokenPrefix', count() FROM tab_folded_constant WHERE hasTokenPrefix(toString(if(c, 1, 0)), '1', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'Bool constant hasTokenPrefix, no index', count() FROM tab_folded_constant WHERE hasTokenPrefix(toString(if(c, 1, 0)), '1', 'array') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;

-- A `Float32` constant is named by its number, so the index is found, and the expression evaluated for a part
-- without the index must keep the `Float32` type: as `Float64`, `0.1` is formatted as `0.10000000149011612`.
CREATE TABLE tab_folded_constant (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_folded_constant SELECT number, 'env' FROM numbers(64);
ALTER TABLE tab_folded_constant ADD INDEX idx(formatRowNoNewline('CSV', tag, toFloat32(0.1))) TYPE text(tokenizer = array);
INSERT INTO tab_folded_constant SELECT number + 64, 'env' FROM numbers(64);

SELECT 'Float32 constant hasTokenMatch', count() FROM tab_folded_constant WHERE hasTokenMatch(formatRowNoNewline('CSV', tag, toFloat32(0.1)), ',0.1$', 'array') SETTINGS use_skip_indexes = 1;
SELECT 'Float32 constant hasTokenMatch, no index', count() FROM tab_folded_constant WHERE hasTokenMatch(formatRowNoNewline('CSV', tag, toFloat32(0.1)), ',0.1$', 'array') SETTINGS use_skip_indexes = 0;
SELECT 'Float32 constant LIKE', count() FROM tab_folded_constant WHERE formatRowNoNewline('CSV', tag, toFloat32(0.1)) LIKE '%,0.1' SETTINGS use_skip_indexes = 1;
SELECT 'Float32 constant LIKE, no index', count() FROM tab_folded_constant WHERE formatRowNoNewline('CSV', tag, toFloat32(0.1)) LIKE '%,0.1' SETTINGS use_skip_indexes = 0;
SELECT 'Float32 constant hasAnyTokens', count() FROM tab_folded_constant WHERE hasAnyTokens(formatRowNoNewline('CSV', tag, toFloat32(0.1)), ['"env",0.1'], 'array') SETTINGS use_skip_indexes = 1;
SELECT 'Float32 constant hasAnyTokens, no index', count() FROM tab_folded_constant WHERE hasAnyTokens(formatRowNoNewline('CSV', tag, toFloat32(0.1)), ['"env",0.1'], 'array') SETTINGS use_skip_indexes = 0;

DROP TABLE tab_folded_constant;
