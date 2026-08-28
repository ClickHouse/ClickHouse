-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/115999
-- A text-search function without an explicit tokenizer takes it from the text index on its haystack.
-- The tokenizer is resolved in the analyzer, so it does not depend on the shape of the query plan: a
-- predicate that a JOIN keeps away from the table scan is tokenized exactly like one that reaches it.

SET enable_full_text_index = 1;
SET enable_analyzer = 1;

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

-- Parallel replicas optimize the local plan on its own, so a plan-level rewrite could not reach this.
SELECT 'parallel replicas';

SELECT a.record_id
FROM t_115999_a AS a INNER JOIN t_115999_b AS b ON a.group_id = b.group_id
WHERE hasAnyTokens(a.shingle_tokens, ['alpha beta gamma']) OR b.category = 'nonexistent'
ORDER BY a.record_id
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;

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
DROP TABLE t_115999_carrier;
DROP TABLE t_115999_side;
