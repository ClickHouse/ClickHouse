-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/115999
-- A text-search predicate OR-combined with a condition on the other side of a JOIN cannot be pushed
-- below the JOIN, so it stays in a WHERE filter above it; the index's tokenizer must still reach it there.

SET enable_full_text_index = 1;

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

SELECT 'default settings';

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

DROP TABLE t_115999_a;
DROP TABLE t_115999_b;

-- An index whose postprocessor normalises the stored tokens: a predicate stranded above the JOIN must be
-- rewritten with the postprocessor too, not only the tokenizer, or it answers a different question than the
-- index does. Each arm is followed by the two executions that cannot use the index at all, which must agree.
SELECT 'postprocessor, predicate stranded above the JOIN';

CREATE TABLE t_115999_pp
(
    id UInt64,
    group_id UInt64,
    val String,
    tokens Array(String),
    INDEX idx_val(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(val)),
    INDEX idx_tokens(tokens) TYPE text(tokenizer = 'array', postprocessor = lower(tokens))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_115999_pb (group_id UInt64, category String) ENGINE = MergeTree ORDER BY group_id;

INSERT INTO t_115999_pp VALUES (1, 1, 'HELLO world', ['ALPHA BETA']), (2, 2, 'other text', ['gamma delta']);
INSERT INTO t_115999_pb VALUES (1, 'x'), (2, 'y');

SELECT 'hasToken';
SELECT p.id FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
WHERE hasToken(p.val, 'hello') OR b.category = 'nonexistent' ORDER BY p.id;
SELECT p.id FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
WHERE hasToken(p.val, 'hello') OR b.category = 'nonexistent' ORDER BY p.id SETTINGS use_skip_indexes = 0;
SELECT p.id FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
WHERE hasToken(p.val, 'hello') OR b.category = 'nonexistent' ORDER BY p.id
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'hasAnyTokens on an array-tokenizer index';
SELECT p.id FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
WHERE hasAnyTokens(p.tokens, ['alpha beta']) OR b.category = 'nonexistent' ORDER BY p.id;
SELECT p.id FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
WHERE hasAnyTokens(p.tokens, ['alpha beta']) OR b.category = 'nonexistent' ORDER BY p.id SETTINGS use_skip_indexes = 0;
SELECT p.id FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
WHERE hasAnyTokens(p.tokens, ['alpha beta']) OR b.category = 'nonexistent' ORDER BY p.id
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'hasPhrase';
SELECT p.id FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
WHERE hasPhrase(p.val, 'hello world') OR b.category = 'nonexistent' ORDER BY p.id;
SELECT p.id FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
WHERE hasPhrase(p.val, 'hello world') OR b.category = 'nonexistent' ORDER BY p.id SETTINGS use_skip_indexes = 0;

-- The same predicate without a JOIN, which the rewrite has always reached: the arms above must match it.
SELECT 'the same predicates without a JOIN';
SELECT id FROM t_115999_pp WHERE hasToken(val, 'hello') ORDER BY id;
SELECT id FROM t_115999_pp WHERE hasAnyTokens(tokens, ['alpha beta']) ORDER BY id;
SELECT id FROM t_115999_pp WHERE hasPhrase(val, 'hello world') ORDER BY id;

-- Row-preserving steps between the JOIN and the stranded predicate (the projection folds into the filter,
-- leaving Sorting and Limit): the walk must carry the indexed column through them. Parallel replicas put
-- other steps there, which the walk does not cross, so this shape is pinned to a local plan.
SELECT 'pass-through steps between the JOIN and the stranded predicate';

SELECT id FROM
(
    SELECT p.id AS id, p.tokens AS tokens, b.category AS category
    FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
    ORDER BY id
    LIMIT 10
)
WHERE hasAnyTokens(tokens, ['alpha beta']) OR category = 'nonexistent'
ORDER BY id
SETTINGS enable_parallel_replicas = 0;

SELECT id FROM
(
    SELECT p.id AS id, p.tokens AS tokens, b.category AS category
    FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
    ORDER BY id
    LIMIT 10
)
WHERE hasAnyTokens(tokens, ['alpha beta']) OR category = 'nonexistent'
ORDER BY id
SETTINGS use_skip_indexes = 0, enable_parallel_replicas = 0;

-- A post-JOIN Expression that carries the indexed column only as a header pass-through, which the walk has
-- to follow as well: this is the shape where tracking the DAG outputs alone loses the column.
SELECT 'a text-search function in the SELECT list above the JOIN';
SELECT p.id, hasAnyTokens(p.tokens, ['alpha beta']) AS m
FROM t_115999_pp AS p INNER JOIN t_115999_pb AS b ON p.group_id = b.group_id
ORDER BY p.id;

DROP TABLE t_115999_pp;
DROP TABLE t_115999_pb;
