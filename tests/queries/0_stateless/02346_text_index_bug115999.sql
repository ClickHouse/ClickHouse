-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/115999
-- A text-search function without an explicit tokenizer takes it from the text index on its haystack.
-- The tokenizer is resolved in the analyzer, so it does not depend on the shape of the query plan: a
-- predicate that a JOIN keeps away from the table scan is tokenized exactly like one that reaches it.
-- No `no-parallel-replicas` tag on purpose: a plan-level rewrite cannot reach the local plan of a
-- replica, so the run with parallel replicas enabled is what pins that this one does.

SET enable_full_text_index = 1;
SET enable_analyzer = 1;
-- `clickhouse-test` randomizes both, and the arms below name the direct-read path they exercise.
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

CREATE TABLE t_115999_a
(
    record_id UInt64,
    group_id UInt64,
    plain_text String,
    shingle_tokens Array(String),
    INDEX idx_plain_text plain_text TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_shingle_tokens shingle_tokens TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY record_id;

CREATE TABLE t_115999_b
(
    group_id UInt64,
    category String
)
ENGINE = MergeTree
ORDER BY group_id;

INSERT INTO t_115999_a VALUES
    (1, 1, 'alpha beta gamma', ['alpha beta gamma']),
    (2, 2, 'fallback', ['delta epsilon zeta']),
    (3, 3, 'alpha beta gamma', ['alpha beta gamma']),
    (4, 4, 'unrelated', ['unrelated example value']);

INSERT INTO t_115999_b VALUES
    (1, 'target'),
    (2, 'other'),
    (3, 'other'),
    (4, 'target');

SELECT 'query_plan_direct_read_from_text_index = 1';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE (hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) AND b.category = 'target')
   OR (hasAnyTokens(a.plain_text, ['fallback']) OR hasAnyTokens(a.shingle_tokens, ['delta epsilon zeta']))
ORDER BY a.record_id;

-- Row-level evaluation must agree, i.e. the fix must not depend on the direct read from the index.
SELECT 'query_plan_direct_read_from_text_index = 0';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE (hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) AND b.category = 'target')
   OR (hasAnyTokens(a.plain_text, ['fallback']) OR hasAnyTokens(a.shingle_tokens, ['delta epsilon zeta']))
ORDER BY a.record_id
SETTINGS query_plan_direct_read_from_text_index = 0;

-- The tokenizer must not depend on what reached the table. Nothing of this predicate does, and with
-- `use_join_disjunctions_push_down = 0` nothing of the one above does either.
SELECT 'nothing pushed below the JOIN';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) OR b.category = 'nonexistent'
ORDER BY a.record_id;

SELECT 'use_join_disjunctions_push_down = 0';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE (hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) AND b.category = 'target')
   OR (hasAnyTokens(a.plain_text, ['fallback']) OR hasAnyTokens(a.shingle_tokens, ['delta epsilon zeta']))
ORDER BY a.record_id
SETTINGS use_join_disjunctions_push_down = 0;

-- Only one of the two indexes is constrained by what reaches the table; the other must still be applied.
SELECT 'one index constrained below the JOIN, the other above';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE hasAnyTokens(a.plain_text, ['fallback'])
  AND (hasAnyTokens(a.shingle_tokens, ['delta epsilon zeta']) OR b.category = 'target')
ORDER BY a.record_id;

SELECT 'OR with a column of the scanned table';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) OR a.group_id = 999
ORDER BY a.record_id;

SELECT 'AND with the other side, no OR';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) AND b.category = 'target'
ORDER BY a.record_id;

-- The explicit-tokenizer form and `has` never depended on the injection; they pin that it stays that way.
SELECT 'explicit tokenizer argument';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE hasAnyTokens(a.shingle_tokens, ['alpha beta gamma'], 'array') OR a.group_id = 999
ORDER BY a.record_id;

SELECT 'has, not a text-search function';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE has(a.shingle_tokens, 'alpha beta gamma') OR a.group_id = 999
ORDER BY a.record_id;

SELECT 'hasAllTokens above the JOIN';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE hasAllTokens(a.shingle_tokens, ['alpha beta gamma']) OR b.category = 'nonexistent'
ORDER BY a.record_id;

-- An unindexed column of the other join side that happens to share the indexed column's name must not be
-- tokenized with this index's tokenizer. Both queries must agree (and return nothing).
SELECT 'same column name on the other join side';

CREATE TABLE t_115999_c (id UInt64, shingle_tokens Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_115999_c VALUES (1, ['alpha beta gamma']);

SELECT c.id
FROM t_115999_a AS a INNER JOIN t_115999_c AS c ON a.record_id = c.id
WHERE hasAnyTokens(a.shingle_tokens, ['alpha beta gamma'])
  AND (hasAnyTokens(c.shingle_tokens, ['alpha beta gamma']) OR a.record_id = 999)
ORDER BY c.id;

SELECT c.id
FROM t_115999_a AS a INNER JOIN t_115999_c AS c ON a.record_id = c.id
WHERE hasAnyTokens(a.shingle_tokens, ['alpha beta gamma'])
  AND (hasAnyTokens(c.shingle_tokens, ['alpha beta gamma']) OR a.record_id = 999)
ORDER BY c.id
SETTINGS use_skip_indexes = 0;

DROP TABLE t_115999_c;

SELECT 'without a JOIN';

SELECT record_id
FROM t_115999_a
WHERE hasAnyTokens(shingle_tokens, ['alpha beta gamma'])
   OR hasAnyTokens(shingle_tokens, ['delta epsilon zeta'])
ORDER BY record_id;

-- The function does not have to be a filter at all.
SELECT 'a text-search function in the SELECT list above the JOIN';

SELECT a.record_id, hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) AS m
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
ORDER BY a.record_id;

-- A subquery renames nothing, so the index still describes the column it passes through.
SELECT 'a subquery between the scan and the predicate';

SELECT id FROM
(
    SELECT a.record_id AS id, a.shingle_tokens AS shingle_tokens, b.category AS category
    FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
    ORDER BY id
    LIMIT 10
)
WHERE hasAnyTokens(shingle_tokens, ['alpha beta gamma']) OR category = 'nonexistent'
ORDER BY id;

DROP TABLE t_115999_a;
DROP TABLE t_115999_b;

-- An index can be defined on an expression or a Map projection, not only on a bare column.
CREATE TABLE t_115999_carrier
(
    id UInt64,
    group_id UInt64,
    s String,
    m Map(String, String),
    INDEX idx_lower_s (lower(s)) TYPE text(tokenizer = ngrams(3)),
    INDEX idx_map_values mapValues(m) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_115999_side (group_id UInt64, category String) ENGINE = MergeTree ORDER BY group_id;

INSERT INTO t_115999_carrier VALUES (1, 1, 'HELLO world', {'k': 'alpha beta'}), (2, 2, 'other text', {'k': 'gamma delta'});
INSERT INTO t_115999_side VALUES (1, 'x'), (2, 'y');

-- Each arm is followed by the same predicate without a JOIN, which has always been rewritten.
SELECT 'expression carrier above the JOIN';
SELECT c.id FROM t_115999_carrier AS c INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
WHERE hasAnyTokens(lower(c.s), ['ell']) OR b.category = 'nonexistent' ORDER BY c.id;
SELECT id FROM t_115999_carrier WHERE hasAnyTokens(lower(s), ['ell']) ORDER BY id;

SELECT 'Map carrier above the JOIN';
SELECT c.id FROM t_115999_carrier AS c INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
WHERE hasAnyTokens(mapValues(c.m), ['alpha beta']) OR b.category = 'nonexistent' ORDER BY c.id;
SELECT id FROM t_115999_carrier WHERE hasAnyTokens(mapValues(m), ['alpha beta']) ORDER BY id;

-- `lo wo` is a phrase of three 3-grams, but not a token of the default tokenizer.
SELECT 'hasPhrase above the JOIN';
SELECT c.id FROM t_115999_carrier AS c INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
WHERE hasPhrase(lower(c.s), 'lo wo') OR b.category = 'nonexistent' ORDER BY c.id;
SELECT id FROM t_115999_carrier WHERE hasPhrase(lower(s), 'lo wo') ORDER BY id;

-- Renamed, but still the scan's column, so the index still describes it.
SELECT 'the indexed column renamed below the JOIN';
SELECT id FROM
(
    SELECT c.id AS id, c.x AS x, b.category AS category
    FROM (SELECT id, group_id, s AS x FROM t_115999_carrier) AS c
    INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
)
WHERE hasAnyTokens(lower(x), ['ell']) OR category = 'nonexistent'
ORDER BY id;

-- Same name, different value: no index describes it, so the default tokenizer applies and nothing matches.
SELECT 'a computed column reusing the indexed column name';
SELECT id FROM
(
    SELECT c.id AS id, c.s AS s, b.category AS category
    FROM (SELECT id, group_id, concat(s, ' zzz') AS s FROM t_115999_carrier) AS c
    INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
)
WHERE hasAnyTokens(lower(s), ['ell']) OR category = 'nonexistent'
ORDER BY id;

SELECT 'a filled join between the scan and the predicate';
CREATE TABLE t_115999_filled (group_id UInt64, category String) ENGINE = Join(ANY, LEFT, group_id);
INSERT INTO t_115999_filled VALUES (1, 'x'), (2, 'y');

SELECT c.id FROM t_115999_carrier AS c ANY LEFT JOIN t_115999_filled AS f USING (group_id)
WHERE hasAnyTokens(mapValues(c.m), ['alpha beta']) OR f.category = 'nonexistent'
ORDER BY c.id;

DROP TABLE t_115999_filled;

-- A subquery can project the indexed expression itself, not only a column of it. Seen from outside, `x`
-- still is `lower(s)`, so the index describes it.
SELECT 'an expression index projected by a subquery';
SELECT id FROM
(
    SELECT c.id AS id, c.x AS x, b.category AS category
    FROM (SELECT id, group_id, lower(s) AS x FROM t_115999_carrier) AS c
    INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
)
WHERE hasAnyTokens(x, ['ell']) OR category = 'nonexistent'
ORDER BY id;
SELECT id FROM t_115999_carrier WHERE hasAnyTokens(lower(s), ['ell']) ORDER BY id;

-- A `mapValues(m)` index also describes the `m['k']` the predicate reads it through.
SELECT 'a Map element above the JOIN';
SELECT c.id FROM t_115999_carrier AS c INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
WHERE hasAnyTokens(c.m['k'], ['alpha beta']) OR b.category = 'nonexistent' ORDER BY c.id;
-- Recognized under conversions too, however many.
SELECT c.id FROM t_115999_carrier AS c INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
WHERE hasAnyTokens(toNullable(c.m['k']), ['alpha beta']) OR b.category = 'nonexistent' ORDER BY c.id;
SELECT c.id FROM t_115999_carrier AS c INNER JOIN t_115999_side AS b ON c.group_id = b.group_id
WHERE hasAnyTokens(toNullable(toLowCardinality(c.m['k'])), ['alpha beta']) OR b.category = 'nonexistent' ORDER BY c.id;
SELECT id FROM t_115999_carrier WHERE hasAnyTokens(m['k'], ['alpha beta']) ORDER BY id;

DROP TABLE t_115999_carrier;

-- A `JSONAllValues(j)` index describes the JSON subcolumns read out of it.
SELECT 'a JSON subcolumn above the JOIN';
CREATE TABLE t_115999_json
(
    id UInt64,
    group_id UInt64,
    j JSON,
    INDEX idx_json JSONAllValues(j) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_json VALUES (1, 1, '{"k": "alpha beta"}'), (2, 2, '{"k": "gamma delta"}');

SELECT t.id FROM t_115999_json AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.j.k::String, ['alpha beta']) OR b.category = 'nonexistent' ORDER BY t.id;
-- A conversion above the cast must not hide the JSON path.
SELECT t.id FROM t_115999_json AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(toNullable(t.j.k::String), ['alpha beta']) OR b.category = 'nonexistent' ORDER BY t.id;
SELECT id FROM t_115999_json WHERE hasAnyTokens(j.k::String, ['alpha beta']) ORDER BY id;

DROP TABLE t_115999_json;

-- An ALIAS column stands for the expression the index is defined on.
SELECT 'an ALIAS column carrying the indexed expression';
CREATE TABLE t_115999_alias
(
    id UInt64,
    group_id UInt64,
    s String,
    lowered String ALIAS lower(s),
    INDEX idx_lower_s (lower(s)) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_alias VALUES (1, 1, 'HELLO world'), (2, 2, 'other text');

SELECT a.id FROM t_115999_alias AS a INNER JOIN t_115999_side AS b ON a.group_id = b.group_id
WHERE hasAnyTokens(a.lowered, ['ell']) OR b.category = 'nonexistent' ORDER BY a.id;
SELECT id FROM t_115999_alias WHERE hasAnyTokens(lowered, ['ell']) ORDER BY id;

DROP TABLE t_115999_alias;

-- A lambda parameter is bound inside the haystack, so it does not stop the expression from resolving.
SELECT 'a higher-order indexed expression above the JOIN';
CREATE TABLE t_115999_lambda
(
    id UInt64,
    group_id UInt64,
    arr Array(String),
    INDEX idx_arr (arrayFilter(x -> x != '', arr)) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_lambda VALUES (1, 1, ['alpha beta', '']), (2, 2, ['gamma delta']);

SELECT l.id FROM t_115999_lambda AS l INNER JOIN t_115999_side AS b ON l.group_id = b.group_id
WHERE hasAnyTokens(arrayFilter(x -> x != '', l.arr), ['alpha beta']) OR b.category = 'nonexistent'
ORDER BY l.id;
SELECT id FROM t_115999_lambda WHERE hasAnyTokens(arrayFilter(x -> x != '', arr), ['alpha beta']) ORDER BY id;

DROP TABLE t_115999_lambda;

-- Every branch of a UNION exposes the same indexed expression of the same table, so the tokenizer does
-- not depend on which branch produced the row.
SELECT 'a UNION ALL between the scan and the predicate';
CREATE TABLE t_115999_union
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_union VALUES (1, ['alpha beta']), (2, ['gamma delta']);

SELECT count() FROM
(
    SELECT tags FROM t_115999_union ORDER BY id LIMIT 10
    UNION ALL
    SELECT tags FROM t_115999_union ORDER BY id LIMIT 10
)
WHERE hasAnyTokens(tags, ['alpha beta']);
SELECT count() FROM t_115999_union WHERE hasAnyTokens(tags, ['alpha beta']);

DROP TABLE t_115999_union;

-- A view is transparent to this pass only once the analyzer has inlined it, so `analyzer_inline_views`
-- decides whether the indexed table is reachable from a predicate the JOIN stranded above it.
SELECT 'an indexed column behind a view';
CREATE TABLE t_115999_viewed
(
    id UInt64,
    group_id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_viewed VALUES (1, 1, ['alpha beta']), (2, 2, ['gamma delta']);

CREATE VIEW v_115999_viewed AS SELECT id, group_id, tags FROM t_115999_viewed;

SELECT count() FROM v_115999_viewed AS v INNER JOIN t_115999_side AS b ON v.group_id = b.group_id
WHERE hasAnyTokens(v.tags, ['alpha beta']) OR b.category = 'nonexistent'
SETTINGS analyzer_inline_views = 1;
-- The oracle reads the table, not the view: through an opaque view only the plan rewrite can supply the
-- tokenizer, and whether it reaches that scan depends on the plan (it does not under parallel replicas).
SELECT count() FROM t_115999_viewed WHERE hasAnyTokens(tags, ['alpha beta']);

-- Tracked gap, prints 0 where the two above print 1: an opaque `StorageView` carries no index
-- definitions, so the stranded predicate keeps the default tokenizer.
SELECT 'the same view left opaque (tracked gap, prints 0)';
SELECT count() FROM v_115999_viewed AS v INNER JOIN t_115999_side AS b ON v.group_id = b.group_id
WHERE hasAnyTokens(v.tags, ['alpha beta']) OR b.category = 'nonexistent'
SETTINGS analyzer_inline_views = 0;

DROP VIEW v_115999_viewed;
DROP TABLE t_115999_viewed;

-- Tracked gap. Only the tokenizer is forwarded here; an index preprocessor or postprocessor reaches a
-- predicate solely through the plan rewrite, which a JOIN keeps it away from. Each pair prints the
-- scan-local answer and then the stranded one, which stays 0 until those transforms are forwarded too.
SELECT 'an index preprocessor above the JOIN (tracked gap, prints 1 then 0)';
CREATE TABLE t_115999_preprocessor
(
    id UInt64,
    group_id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_preprocessor VALUES (1, 1, 'HELLO');

SELECT count() FROM t_115999_preprocessor WHERE hasAnyTokens(s, ['hello']);
SELECT count() FROM t_115999_preprocessor AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.s, ['hello']) OR b.category = 'nonexistent';

DROP TABLE t_115999_preprocessor;

SELECT 'an index postprocessor above the JOIN (tracked gap, prints 1 then 0)';
CREATE TABLE t_115999_postprocessor
(
    id UInt64,
    group_id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_postprocessor VALUES (1, 1, 'HELLO');

SELECT count() FROM t_115999_postprocessor WHERE hasAnyTokens(s, ['hello']);
SELECT count() FROM t_115999_postprocessor AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.s, ['hello']) OR b.category = 'nonexistent';

DROP TABLE t_115999_postprocessor;

-- `hasPhrase` matches tokens by position and rejects a tokenizer that cannot number them that way, so
-- forwarding the index tokenizer here would make the function throw where it answers with its default.
SELECT 'a tokenizer the function does not accept';
CREATE TABLE t_115999_unsupported
(
    id UInt64,
    group_id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = sparseGrams(3))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_unsupported VALUES (1, 1, 'hello world');

SELECT count() FROM t_115999_unsupported WHERE hasPhrase(s, 'hello world');
SELECT count() FROM t_115999_unsupported AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasPhrase(t.s, 'hello world') OR b.category = 'nonexistent';
-- The same index still forwards its tokenizer to a function that does accept it.
SELECT count() FROM t_115999_unsupported AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.s, ['hello']) OR b.category = 'nonexistent';

DROP TABLE t_115999_unsupported;

-- An expression pairing two occurrences of the table is the indexed expression of neither, though it
-- serializes to the same name once the aliases are dropped.
SELECT 'a self-join mixing two occurrences of the indexed table';
CREATE TABLE t_115999_self
(
    id UInt64,
    first_name String,
    last_name String,
    INDEX idx_name concat(first_name, last_name) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_self VALUES (1, 'hel', 'lo'), (2, 'wor', 'ld');

-- Prints 0: 'hel' and 'ld' never share a row.
SELECT count() FROM t_115999_self AS a, t_115999_self AS b
WHERE a.id = 1 AND b.id = 2 AND hasAnyTokens(concat(a.first_name, b.last_name), ['ell']);
-- One occurrence is still the indexed expression, so the tokenizer is forwarded.
SELECT count() FROM t_115999_self AS a, t_115999_self AS b
WHERE b.id = 2 AND (hasAnyTokens(concat(a.first_name, a.last_name), ['ell']) OR b.id = 999);
SELECT count() FROM t_115999_self WHERE hasAnyTokens(concat(first_name, last_name), ['ell']);

DROP TABLE t_115999_self;

-- A conversion carries the indexed expression only when it cannot change a value, the same rule the scan
-- matches an index column under.
SELECT 'a conversion around the indexed column';
CREATE TABLE t_115999_converted
(
    id UInt64,
    group_id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = ngrams(2))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_converted VALUES (1, 1, 'abc');

SELECT count() FROM t_115999_converted AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(toNullable(t.s), ['ab']) OR b.category = 'nonexistent';
-- Conversions nest, so they are peeled to the bottom rather than one layer.
SELECT count() FROM t_115999_converted AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(toNullable(toLowCardinality(t.s)), ['ab']) OR b.category = 'nonexistent';
-- Prints 0: no index is defined on the truncated value.
SELECT count() FROM t_115999_converted AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(CAST(t.s, 'FixedString(3)'), ['ab']) OR b.category = 'nonexistent';
SELECT count() FROM t_115999_converted WHERE hasAnyTokens(s, ['ab']);

DROP TABLE t_115999_converted;

-- The lookup key only selects which value is read, so it takes no part in matching the index, even when
-- it comes from the other side of the join.
SELECT 'a Map element whose key comes from the joined table';
CREATE TABLE t_115999_keyed
(
    id UInt64,
    group_id UInt64,
    m Map(String, String),
    INDEX idx_m mapValues(m) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_115999_keys
(
    group_id UInt64,
    k String
)
ENGINE = MergeTree
ORDER BY group_id;

INSERT INTO t_115999_keyed VALUES (1, 1, map('k', 'alpha beta'));
INSERT INTO t_115999_keys VALUES (1, 'k');

SELECT count() FROM t_115999_keyed AS t INNER JOIN t_115999_keys AS kk ON t.group_id = kk.group_id
WHERE hasAnyTokens(t.m[kk.k], ['alpha beta']);
-- The same lookup reached through a subquery projection, and through a conversion around it.
SELECT count() FROM
(
    SELECT t.m[kk.k] AS x FROM t_115999_keyed AS t CROSS JOIN t_115999_keys AS kk LIMIT 10
)
WHERE hasAnyTokens(x, ['alpha beta']);
SELECT count() FROM t_115999_keyed AS t INNER JOIN t_115999_keys AS kk ON t.group_id = kk.group_id
WHERE hasAnyTokens(toNullable(t.m[kk.k]), ['alpha beta']);
SELECT count() FROM t_115999_keyed WHERE hasAnyTokens(m['k'], ['alpha beta']);

DROP TABLE t_115999_keys;
DROP TABLE t_115999_keyed;

-- Only the outer key is exempt. The Map it reads must resolve on its own, or a nested lookup keyed from
-- the joined table would match an index covering a different inner map.
SELECT 'a nested Map whose inner key comes from the joined table';
CREATE TABLE t_115999_nested
(
    id UInt64,
    k String,
    mm Map(String, Map(String, String)),
    INDEX idx_mm mapValues(mm[k]) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_115999_nested_keys (k String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_115999_nested VALUES (1, 'a', map('a', map('x', 'hello'), 'b', map('x', 'world')));
INSERT INTO t_115999_nested_keys VALUES ('b');

-- Prints 0: the index covers `mm['a']`, not the `mm['b']` this reads.
SELECT count() FROM t_115999_nested AS t CROSS JOIN t_115999_nested_keys AS kk
WHERE hasAnyTokens(t.mm[kk.k]['x'], ['orl']) OR kk.k = 'zzz';
-- The table's own key is the indexed expression, so the tokenizer is forwarded.
SELECT count() FROM t_115999_nested AS t CROSS JOIN t_115999_nested_keys AS kk
WHERE hasAnyTokens(t.mm[t.k]['x'], ['ell']) OR kk.k = 'zzz';

DROP TABLE t_115999_nested_keys;
DROP TABLE t_115999_nested;

-- A lambda parameter is the indexed expression's own only when the lambda is inside the haystack. One
-- captured from an enclosing lambda names something else, however it is spelled.
SELECT 'a lambda capture named like the indexed column';
CREATE TABLE t_115999_capture
(
    id UInt64,
    suffix String,
    tags Array(String),
    INDEX idx_tags arrayMap(y -> concat(y, suffix), tags) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_capture VALUES (1, '!', ['alpha beta']);

-- Both print [0]: the capture supplies '?', so neither reads the indexed `concat(y, suffix)`, and the
-- answer cannot depend on what that parameter is called.
SELECT arrayMap(suffix -> hasAnyTokens(arrayMap(y -> concat(y, suffix), tags), ['alpha beta?']), ['?'])
FROM t_115999_capture;
SELECT arrayMap(other -> hasAnyTokens(arrayMap(y -> concat(y, other), tags), ['alpha beta?']), ['?'])
FROM t_115999_capture;
-- The indexed expression itself still forwards its tokenizer.
SELECT hasAnyTokens(arrayMap(y -> concat(y, suffix), tags), ['alpha beta!']) FROM t_115999_capture;

DROP TABLE t_115999_capture;

-- Union arms are agreed one projection at a time, so two columns of one haystack say nothing about how
-- they relate inside an arm. Whatever is forwarded must not depend on the order of the arms.
SELECT 'a multi-column haystack above UNION ALL arms';
CREATE TABLE t_115999_arms
(
    id UInt64,
    a String,
    b String,
    INDEX idx_ab concat(a, b) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_arms VALUES (1, 'hel', 'lo'), (2, 'wor', 'ld');

-- Prints 1: the orderings agree. The count is not pinned, and is not uniform either - the plan still
-- reaches the scan of the plain arm and tokenizes its rows with the index while the joined arm uses the
-- default. That residual split is the tracked gap; what this pins is that arm order cannot change it.
SELECT
(
    SELECT count() FROM
    (
        SELECT a AS x, b AS y FROM t_115999_arms ORDER BY id LIMIT 10
        UNION ALL
        SELECT t1.a AS x, t2.b AS y FROM t_115999_arms AS t1 CROSS JOIN t_115999_arms AS t2 ORDER BY t1.id LIMIT 10
    )
    WHERE hasAnyTokens(concat(x, y), ['ell'])
) = (
    SELECT count() FROM
    (
        SELECT t1.a AS x, t2.b AS y FROM t_115999_arms AS t1 CROSS JOIN t_115999_arms AS t2 ORDER BY t1.id LIMIT 10
        UNION ALL
        SELECT a AS x, b AS y FROM t_115999_arms ORDER BY id LIMIT 10
    )
    WHERE hasAnyTokens(concat(x, y), ['ell'])
);
-- One column through the union still forwards.
SELECT count() FROM
(
    SELECT concat(a, b) AS z FROM t_115999_arms ORDER BY id LIMIT 10
    UNION ALL
    SELECT concat(a, b) AS z FROM t_115999_arms ORDER BY id LIMIT 10
)
WHERE hasAnyTokens(z, ['ell']);

DROP TABLE t_115999_arms;

-- The flattened spelling of a JSON path can equally name a declared column. Such a column is what the
-- predicate reads, so the index on the JSON column says nothing about it.
SELECT 'a declared column spelled like a JSON path';
CREATE TABLE t_115999_shadowed
(
    group_id UInt64,
    j JSON(typed String),
    `j.k` String,
    `j.t` Tuple(m String),
    c Tuple(j JSON(k String)),
    INDEX idx_j JSONAllValues(j) TYPE text(tokenizer = ngrams(3)),
    INDEX idx_c JSONAllValues(c.j) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY group_id;

INSERT INTO t_115999_shadowed VALUES (1, '{"typed":"hello","other":"hello"}', 'world', tuple('world'), tuple('{"k":"hello"}'));

-- The first three print 0: `j.k` and `j.t.m` read the declared columns, so the default applies.
SELECT count() FROM t_115999_shadowed AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.`j.k`, ['orl']) OR b.category = 'nonexistent';
SELECT count() FROM t_115999_shadowed AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.`j.k`, ['orl'], 'splitByNonAlpha') OR b.category = 'nonexistent';
SELECT count() FROM t_115999_shadowed AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.`j.t`.m, ['orl']) OR b.category = 'nonexistent';
-- Both print 1: a path of the indexed JSON column still forwards, declared typed or not, so the refusal
-- above is about which column owns the name rather than its shape.
SELECT count() FROM t_115999_shadowed AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.j.other::String, ['ell']) OR b.category = 'nonexistent';
SELECT count() FROM t_115999_shadowed AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.j.typed, ['ell']) OR b.category = 'nonexistent';
-- The indexed JSON value may itself sit inside a physical column.
SELECT count() FROM t_115999_shadowed AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.c.j.k, ['ell']) OR b.category = 'nonexistent';

DROP TABLE t_115999_shadowed;

-- Two subcolumns of one physical column flattening to the same name: a sibling field wins resolution.
SELECT 'a sibling field shadowing a JSON path in one Tuple';
CREATE TABLE t_115999_sibling
(
    group_id UInt64,
    c Tuple(`j.k` String, j JSON(k String)),
    d Tuple(t Tuple(`j.k` String, j JSON(k String))),
    INDEX idx_c JSONAllValues(c.j) TYPE text(tokenizer = ngrams(3)),
    INDEX idx_d JSONAllValues(d.t.j) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY group_id;

INSERT INTO t_115999_sibling VALUES (1, tuple('world', '{"k":"hello"}'), tuple(tuple('world', '{"k":"hello"}')));

-- Both print 0: `c.j.k` reads the sibling string, which the index on `c.j` does not describe.
SELECT count() FROM t_115999_sibling AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.c.j.k, ['orl']) OR b.category = 'nonexistent';
SELECT count() FROM t_115999_sibling AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.c.j.k, ['orl'], 'splitByNonAlpha') OR b.category = 'nonexistent';
-- The same one `Tuple` deeper.
SELECT count() FROM t_115999_sibling AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.d.t.j.k, ['orl']) OR b.category = 'nonexistent';
SELECT count() FROM t_115999_sibling AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(t.d.t.j.k, ['orl'], 'splitByNonAlpha') OR b.category = 'nonexistent';

DROP TABLE t_115999_sibling;

-- A subcolumn spelled as a function is normalized before this pass runs, so both spellings forward.
SELECT 'a subcolumn read through tupleElement or variantElement';
CREATE TABLE t_115999_element
(
    group_id UInt64,
    tp Tuple(k String),
    vr Variant(String, UInt64),
    INDEX idx_tp tp.k TYPE text(tokenizer = ngrams(3)),
    INDEX idx_vr vr.String TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY group_id;

INSERT INTO t_115999_element VALUES (1, tuple('hello'), 'hello');

-- Pinned: the function spelling is normalized by `optimize_functions_to_subcolumns`, which the test
-- runner randomizes. With it off the spelling reads the default tokenizer at the scan too, consistently.
SELECT count() FROM t_115999_element AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(tupleElement(t.tp, 'k'), ['ell']) OR b.category = 'nonexistent'
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_115999_element AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(tupleElement(t.tp, 1), ['ell']) OR b.category = 'nonexistent'
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_115999_element AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(variantElement(t.vr, 'String'), ['ell']) OR b.category = 'nonexistent'
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_115999_element WHERE hasAnyTokens(tp.k, ['ell']);
-- Tracked gap, both print 0: over a subquery output the element access is never normalized, so the
-- function spelling disagrees with the direct one above.
SELECT count() FROM (SELECT tp FROM t_115999_element LIMIT 10) AS sub CROSS JOIN t_115999_side AS b
WHERE hasAnyTokens(tupleElement(sub.tp, 'k'), ['ell']) OR b.category = 'nonexistent';
SELECT count() FROM (SELECT vr FROM t_115999_element LIMIT 10) AS sub CROSS JOIN t_115999_side AS b
WHERE hasAnyTokens(variantElement(sub.vr, 'String'), ['ell']) OR b.category = 'nonexistent';

DROP TABLE t_115999_element;

-- An index can be defined on an expression that itself contains a text-search call. Forwarding a
-- tokenizer into that nested call changes the enclosing haystack, so the index picked for the
-- enclosing call must be the one that describes the rewritten expression, whichever of the two the
-- traversal reaches first. Spelling the nested tokenizer out by hand must not change any answer.
SELECT 'a text-search call nested inside the haystack of another';
CREATE TABLE t_115999_nested
(
    id UInt64,
    group_id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = ngrams(3)),
    INDEX idx_expr if(hasAnyTokens(s, ['ell']), s, '') TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_115999_nested_spelled
(
    id UInt64,
    group_id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = ngrams(3)),
    INDEX idx_expr if(hasAnyTokens(s, ['ell'], 'ngrams(3)'), s, '') TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_115999_nested VALUES (1, 1, 'hello world');
INSERT INTO t_115999_nested_spelled VALUES (1, 1, 'hello world');

SELECT hasAnyTokens(if(hasAnyTokens(s, ['ell']), s, ''), ['hello world']) FROM t_115999_nested;
SELECT hasAnyTokens(if(hasAnyTokens(s, ['ell'], 'ngrams(3)'), s, ''), ['hello world']) FROM t_115999_nested;
SELECT hasAnyTokens(if(hasAnyTokens(s, ['ell']), s, ''), ['hello world']) FROM t_115999_nested_spelled;
SELECT hasAnyTokens(if(hasAnyTokens(s, ['ell'], 'ngrams(3)'), s, ''), ['hello world']) FROM t_115999_nested_spelled;
SELECT hasAllTokens(if(hasAnyTokens(s, ['ell']), s, ''), ['hello world']) FROM t_115999_nested;
SELECT hasAllTokens(if(hasAnyTokens(s, ['ell'], 'ngrams(3)'), s, ''), ['hello world']) FROM t_115999_nested;

SELECT count() FROM t_115999_nested AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(if(hasAnyTokens(t.s, ['ell']), t.s, ''), ['hello world']) OR b.category = 'nonexistent';
SELECT count() FROM t_115999_nested AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(if(hasAnyTokens(t.s, ['ell'], 'ngrams(3)'), t.s, ''), ['hello world']) OR b.category = 'nonexistent';
SELECT count() FROM t_115999_nested_spelled AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(if(hasAnyTokens(t.s, ['ell']), t.s, ''), ['hello world']) OR b.category = 'nonexistent';
SELECT count() FROM t_115999_nested_spelled AS t INNER JOIN t_115999_side AS b ON t.group_id = b.group_id
WHERE hasAnyTokens(if(hasAnyTokens(t.s, ['ell'], 'ngrams(3)'), t.s, ''), ['hello world']) OR b.category = 'nonexistent';

-- The nested call sits in a subquery projection, which the enclosing call reaches only through the
-- join tree -- a subtree the traversal visits after the projection holding the enclosing call.
SELECT hasAnyTokens(e, ['hello world']) FROM (SELECT if(hasAnyTokens(s, ['ell']), s, '') AS e FROM t_115999_nested);
SELECT hasAnyTokens(e, ['hello world']) FROM (SELECT if(hasAnyTokens(s, ['ell'], 'ngrams(3)'), s, '') AS e FROM t_115999_nested);
SELECT hasAnyTokens(e, ['hello world']) FROM (SELECT if(hasAnyTokens(s, ['ell']), s, '') AS e FROM t_115999_nested_spelled);
SELECT hasAnyTokens(e, ['hello world']) FROM (SELECT if(hasAnyTokens(s, ['ell'], 'ngrams(3)'), s, '') AS e FROM t_115999_nested_spelled);

DROP TABLE t_115999_nested;
DROP TABLE t_115999_nested_spelled;

-- A Distributed table carries no indexes of its own, so the initiator has to reach the shards' local
-- table by name; otherwise a predicate it evaluates itself disagrees with the shard-local scans.
SELECT 'a Distributed haystack, predicate on the initiator';
CREATE TABLE t_115999_local
(
    id UInt64,
    group_id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_115999_dist AS t_115999_local
ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_115999_local, rand());

INSERT INTO t_115999_local VALUES (1, 1, ['alpha beta']), (2, 2, ['gamma delta']);

SELECT d.id FROM t_115999_dist AS d INNER JOIN t_115999_side AS b ON d.group_id = b.group_id
WHERE hasAnyTokens(d.tags, ['alpha beta']) OR b.category = 'nonexistent' ORDER BY d.id;
SELECT r.id FROM remote('127.0.0.1', currentDatabase(), t_115999_local) AS r
INNER JOIN t_115999_side AS b ON r.group_id = b.group_id
WHERE hasAnyTokens(r.tags, ['alpha beta']) OR b.category = 'nonexistent' ORDER BY r.id;
SELECT id FROM t_115999_local WHERE hasAnyTokens(tags, ['alpha beta']) ORDER BY id;

-- Every `remote()` node builds its own `Distributed` instance, so the branches of the union agree on the
-- table the indexes are defined on, not on the storage the branch reads.
SELECT 'a UNION ALL of two remote() reads of one table';
SELECT count() FROM
(
    SELECT tags FROM remote('127.0.0.1', currentDatabase(), t_115999_local) ORDER BY id LIMIT 10
    UNION ALL
    SELECT tags FROM remote('127.0.0.1', currentDatabase(), t_115999_local) ORDER BY id LIMIT 10
)
WHERE hasAnyTokens(tags, ['alpha beta']);
SELECT count() FROM t_115999_local WHERE hasAnyTokens(tags, ['alpha beta']);

DROP TABLE t_115999_dist;
DROP TABLE t_115999_local;
DROP TABLE t_115999_side;
