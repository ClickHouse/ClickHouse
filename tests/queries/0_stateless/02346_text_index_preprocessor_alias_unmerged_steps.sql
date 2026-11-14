-- Tags: no-parallel-replicas
-- With parallel replicas, the steps above the subquery are not rewritten.
-- Random settings limits: optimize_if_transform_strings_to_enum=(0, 0)
-- The enum rewrite changes an `if` ALIAS expression, so it no longer matches the index expression.

-- Text-search functions on an ALIAS column use the text index preprocessor, tokenizer and postprocessor also when
-- the step computing the column is not merged into the step that evaluates the function.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    provider Nullable(String),
    name String ALIAS ifNull(provider, 'default_name'),
    INDEX idx(name) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(name))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab(provider) VALUES ('Hello World'), ('FOO bar'), (NULL);

SELECT count() FROM tab WHERE hasToken(name, 'HELLO') SETTINGS query_plan_enable_optimizations = 0;
SELECT count() FROM tab WHERE hasToken(name, 'HELLO') SETTINGS query_plan_merge_expressions = 0;
SELECT count() FROM tab WHERE hasToken(name, 'DEFAULT') SETTINGS query_plan_enable_optimizations = 0;
SELECT count() FROM tab WHERE hasAllTokens(name, 'HELLO WORLD') SETTINGS query_plan_enable_optimizations = 0;
SELECT count() FROM tab WHERE hasPhrase(name, 'HELLO WORLD') SETTINGS query_plan_enable_optimizations = 0;
SELECT count() FROM tab WHERE hasToken(name, 'HELLO') SETTINGS query_plan_enable_optimizations = 0, use_skip_indexes = 0;
SELECT provider, hasToken(name, 'HELLO') FROM tab ORDER BY provider DESC NULLS LAST SETTINGS query_plan_enable_optimizations = 0;
SELECT countIf(hasToken(name, 'HELLO')) FROM tab WHERE provider IS NOT NULL SETTINGS query_plan_enable_optimizations = 0;
SELECT x, hasToken(x, 'HELLO') FROM (SELECT name AS x FROM tab) ORDER BY x;

DROP TABLE tab;

CREATE TABLE tab
(
    s Nullable(String),
    a String ALIAS ifNull(s, ''),
    INDEX idx(a) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab(s) VALUES ('foo-qux'), ('foo-bar baz');

SELECT count() FROM tab WHERE hasAnyTokens(a, 'foo-qux') SETTINGS query_plan_enable_optimizations = 0;
SELECT s, hasAnyTokens(a, 'foo-qux') FROM tab ORDER BY s SETTINGS query_plan_enable_optimizations = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    provider Nullable(String),
    name String ALIAS ifNull(provider, 'default_name'),
    INDEX idx(name) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(name))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab(provider) VALUES ('Hello World'), ('FOO bar'), (NULL);

SELECT count() FROM tab WHERE hasToken(name, 'HELLO') SETTINGS query_plan_enable_optimizations = 0;
SELECT count() FROM tab WHERE hasAnyTokens(name, 'HELLO') SETTINGS query_plan_enable_optimizations = 0;
SELECT provider, hasToken(name, 'HELLO') FROM tab ORDER BY provider DESC NULLS LAST SETTINGS query_plan_enable_optimizations = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    src String,
    a String ALIAS arrayStringConcat(arrayMap(s -> concat(s, 'x'), splitByChar(' ', src)), ' '),
    INDEX idx(a) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(a))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab(src) VALUES ('Hello World'), ('FOO bar');

SELECT count() FROM tab WHERE hasToken(a, 'HELLOX') SETTINGS query_plan_enable_optimizations = 0;
SELECT src, hasToken(a, 'HELLOX') FROM tab ORDER BY src SETTINGS query_plan_enable_optimizations = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    src String,
    a String ALIAS arrayStringConcat(arrayMap(s -> concat(s, src), splitByChar(' ', src)), ' '),
    INDEX idx(a) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(a))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab(src) VALUES ('Hi');

SELECT count() FROM tab WHERE hasToken(a, 'HIHI') SETTINGS query_plan_enable_optimizations = 0;

DROP TABLE tab;

CREATE TABLE tab
(
    src String,
    a String ALIAS if(src = 'foo', 'yes', 'no'),
    INDEX idx_src(src) TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_a(a) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(a))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab(src) VALUES ('foo');

SELECT hasToken(a, 'YES') FROM tab WHERE src = 'foo' AND a != '' SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;
SELECT hasToken(a, 'YES') FROM tab WHERE src = 'foo' AND a != '' SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

-- Index analysis removes casts and `materialize`, so repeating them does not prevent matching the index expression.
CREATE TABLE tab
(
    s String,
    INDEX idx(concat(s, lower(s))) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES ('FOO-QUX ');

SELECT hasAnyTokens(concat(CAST(s, 'String'), lower(CAST(s, 'String'))), 'FOO') FROM tab;
SELECT hasAnyTokens(concat(materialize(s), lower(materialize(s))), 'FOO') FROM tab;
SELECT hasAnyTokens(concat(CAST(x, 'String'), lower(CAST(x, 'String'))), 'FOO') FROM (SELECT s AS x FROM tab ORDER BY x LIMIT 1);
SELECT count() FROM tab WHERE hasAnyTokens(concat(CAST(s, 'String'), lower(CAST(s, 'String'))), 'FOO') SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0;

DROP TABLE tab;

-- `m[key]` matches the index on `mapValues(m)` whatever the key is, also when the key or `m[key]` is computed in a step below.
CREATE TABLE tab
(
    m Map(String, String),
    k String,
    v String ALIAS m[multiIf(k = 'A', 'a', k = 'B', 'b', k = 'C', 'c', k = 'D', 'd', k = 'E', 'e', k)],
    INDEX idx(mapValues(m)) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES (map('a', 'FOO-QUX '), 'A');

SELECT hasAnyTokens(m[multiIf(k = 'A', 'a', k = 'B', 'b', k = 'C', 'c', k = 'D', 'd', k = 'E', 'e', k)], 'FOO') FROM tab;
SELECT hasAnyTokens(m[key], 'FOO') FROM (SELECT m, multiIf(k = 'A', 'a', k = 'B', 'b', k = 'C', 'c', k = 'D', 'd', k = 'E', 'e', k = 'F', 'f', k) AS key FROM tab ORDER BY key LIMIT 1);
SELECT hasAnyTokens(v, 'FOO') FROM (SELECT m[multiIf(k = 'A', 'a', k = 'B', 'b', k = 'C', 'c', k = 'D', 'd', k = 'E', 'e', k = 'F', 'f', k)] AS v FROM tab ORDER BY v LIMIT 1);
SELECT count() FROM tab WHERE hasAnyTokens(m[multiIf(k = 'A', 'a', k = 'B', 'b', k = 'C', 'c', k = 'D', 'd', k = 'E', 'e', k)], 'FOO') SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0;
SELECT hasAnyTokens(v, 'FOO') FROM (SELECT m[multiIf(k = 'A', 'a', k = 'B', 'b', k = 'C', 'c', k = 'D', 'd', k = 'E', 'e', k)] AS v FROM tab ORDER BY v LIMIT 1);
SELECT hasAnyTokens(v, 'FOO') FROM tab SETTINGS query_plan_enable_optimizations = 0;
SELECT count() FROM tab WHERE hasAnyTokens(v, 'FOO') SETTINGS query_plan_enable_optimizations = 0, use_skip_indexes = 0;

DROP TABLE tab;

-- A function of `m['a']` matches the index on `mapKeys(m)`, and one of `j.a` the index on `JSONAllPaths(j)`, also when computed below.
CREATE TABLE tab
(
    m Map(String, String),
    j JSON(a String),
    v String ALIAS reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(m['a'])))))))))))))))))),
    w String ALIAS reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(reverse(j.a)))))))))))))))))),
    INDEX idx_m(mapKeys(m)) TYPE text(tokenizer = splitByString([' '])),
    INDEX idx_j(JSONAllPaths(j)) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab(m, j) VALUES (map('a', 'FOO-QUX '), '{"a" : "FOO-QUX "}');

SELECT hasAnyTokens(v, 'FOO'), hasAnyTokens(w, 'FOO') FROM tab SETTINGS query_plan_enable_optimizations = 0;
SELECT hasAnyTokens(v, 'FOO'), hasAnyTokens(w, 'FOO') FROM (SELECT v, w FROM tab ORDER BY v LIMIT 1);
SELECT count() FROM tab WHERE hasAnyTokens(v, 'FOO') SETTINGS query_plan_enable_optimizations = 0, use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(w, 'FOO') SETTINGS query_plan_enable_optimizations = 0, use_skip_indexes = 0;

DROP TABLE tab;

-- Steps referencing a column twice share subexpressions: matching a function on the composed DAG must not expand them.
CREATE TABLE tab
(
    src String,
    INDEX idx(src) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(src))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab VALUES ('x');

SELECT hasAnyTokens(x, 'x') FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT src AS x FROM tab) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) SETTINGS max_memory_usage = '500M';
SELECT hasAnyTokens(x, 'x') FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT src AS x FROM tab))))))))))))))))))) SETTINGS query_plan_enable_optimizations = 0, max_memory_usage = '500M';
SELECT hasAnyTokens(x, 'x') FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT if(x = 'a', 'b', x) AS x FROM (SELECT toString(rand()) AS x FROM tab) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) SETTINGS max_memory_usage = '500M';
SELECT hasAnyTokens(concat(src, x), 'x') FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, if(x = 'a', 'b', x) AS x FROM (SELECT src, toString(rand()) AS x FROM tab) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) SETTINGS max_memory_usage = '500M';

SELECT hasAnyTokens(x, 'x') FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT arrayStringConcat(arrayMap(y -> if(y = x, x, if(y > x, x, y)), ['a'])) AS x FROM (SELECT src AS x FROM tab) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1) ORDER BY x LIMIT 1);

DROP TABLE tab;
