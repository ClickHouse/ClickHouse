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

SELECT 'use_skip_indexes = 0';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE (hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) AND b.category = 'target')
   OR (hasAnyTokens(a.plain_text, ['fallback']) OR hasAnyTokens(a.shingle_tokens, ['delta epsilon zeta']))
ORDER BY a.record_id
SETTINGS use_skip_indexes = 0;

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
