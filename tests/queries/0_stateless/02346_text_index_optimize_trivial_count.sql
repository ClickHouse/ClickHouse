-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;

CREATE TABLE tab (
	id UInt64,
	text String,
	INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES tab;
INSERT INTO tab SELECT number, if(number % 2 = 0, 'alpha beta', 'gamma delta') FROM numbers(1000);
INSERT INTO tab SELECT number, if(number % 4 = 0, 'alpha epsilon', 'zeta') FROM numbers(1000);

SELECT '-- fires: single-token hasToken / hasAnyTokens / hasAllTokens';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasAnyTokens(text, ['alpha'])) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha'])) WHERE explain LIKE '%ReadFromTextIndexCount%';

SELECT '-- result matches the normal path';
SELECT count() FROM tab WHERE hasToken(text, 'alpha');
SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab WHERE hasToken(text, 'zeta');
SELECT count() FROM tab WHERE hasToken(text, 'missing');

SELECT '-- disabled by query_plan_optimize_count_from_text_index = 0';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS query_plan_optimize_count_from_text_index = 0) WHERE explain LIKE '%Trivial count from text index%';

SELECT '-- disabled by the parent optimize_trivial_count_query = 0';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS optimize_trivial_count_query = 0) WHERE explain LIKE '%Trivial count from text index%';

SELECT '-- fires: multi-token hasAnyTokens (union)';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasAnyTokens(text, ['alpha', 'zeta'])) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab WHERE hasAnyTokens(text, ['alpha', 'zeta']);
SELECT count() FROM tab WHERE hasAnyTokens(text, ['alpha', 'zeta']) SETTINGS query_plan_optimize_count_from_text_index = 0;

SELECT '-- fires: multi-token hasAllTokens (intersection)';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha', 'beta'])) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha', 'beta']);
SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha', 'zeta']);
SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha', 'zeta']) SETTINGS query_plan_optimize_count_from_text_index = 0;

SELECT '-- does not fire: residual non-text predicate';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha') AND id > 10) WHERE explain LIKE '%Trivial count from text index%';
SELECT count() FROM tab WHERE hasToken(text, 'alpha') AND id > 10;

SELECT '-- does not fire: not a bare count()';
SELECT count(explain) FROM (EXPLAIN SELECT id FROM tab WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%Trivial count from text index%';

DROP TABLE tab;
