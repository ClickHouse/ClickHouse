-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET max_rows_to_group_by = 0; -- make_distributed_plan rejects a nonzero limit
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;

SELECT 'Inject trivial count optimization from the text index into the query plan';

CREATE TABLE tab (
	id UInt64,
	text String,
	INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

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
SELECT count() FROM tab WHERE hasToken(text, 'zeta') SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab WHERE hasToken(text, 'missing');
SELECT count() FROM tab WHERE hasToken(text, 'missing') SETTINGS query_plan_optimize_count_from_text_index = 0;

SELECT '-- disabled by query_plan_optimize_count_from_text_index = 0';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS query_plan_optimize_count_from_text_index = 0) WHERE explain LIKE '%Trivial count from text index%';

SELECT '-- disabled by the parent optimize_trivial_count_query = 0';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS optimize_trivial_count_query = 0) WHERE explain LIKE '%Trivial count from text index%';

SELECT '-- disabled by the parent query_plan_direct_read_from_text_index = 0';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS query_plan_direct_read_from_text_index = 0) WHERE explain LIKE '%Trivial count from text index%';
SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- disabled for shipped plans (ReadFromTextIndexCount is not serializable)';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS make_distributed_plan = 1) WHERE explain LIKE '%Trivial count from text index%';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS serialize_query_plan = 1) WHERE explain LIKE '%Trivial count from text index%';

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

SELECT 'Multi-block postings (small block size exercises read_postings, block skipping, intersection)';

CREATE TABLE tab_blocks (
	id UInt64,
	text String,
	-- Compression + a small block size split 'common'/'other' into many posting blocks (the read_postings
	-- loop and rank()-based skipping only run for multi-block, unfolded tokens); 'rare' stays single-block
	-- and folds into the baseline. Index arguments override the randomized table settings.
	INDEX idx text TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 128)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_blocks SELECT number,
	arrayStringConcat(arrayFilter(x -> x != '', [
		if(number % 2 = 0, 'common', ''),
		if(number % 2 = 1, 'other', ''),
		if(number < 1000 AND number % 100 = 0, 'rare', '')]), ' ')
FROM numbers(5000);

SELECT '-- fires with a multi-block token';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_blocks WHERE hasAllTokens(text, ['rare', 'common'])) WHERE explain LIKE '%ReadFromTextIndexCount%';

SELECT '-- results match the reader (each pair: optimization on, then off)';
SELECT count() FROM tab_blocks WHERE hasAllTokens(text, ['rare', 'common']);
SELECT count() FROM tab_blocks WHERE hasAllTokens(text, ['rare', 'common']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab_blocks WHERE hasAllTokens(text, ['common', 'other']);
SELECT count() FROM tab_blocks WHERE hasAllTokens(text, ['common', 'other']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab_blocks WHERE hasAllTokens(text, ['rare', 'common', 'other']);
SELECT count() FROM tab_blocks WHERE hasAllTokens(text, ['rare', 'common', 'other']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab_blocks WHERE hasAnyTokens(text, ['common', 'other']);
SELECT count() FROM tab_blocks WHERE hasAnyTokens(text, ['common', 'other']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab_blocks WHERE hasAnyTokens(text, ['rare', 'common']);
SELECT count() FROM tab_blocks WHERE hasAnyTokens(text, ['rare', 'common']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab_blocks WHERE hasToken(text, 'common');
SELECT count() FROM tab_blocks WHERE hasToken(text, 'common') SETTINGS query_plan_optimize_count_from_text_index = 0;

DROP TABLE tab_blocks;

SELECT 'Array and Map carriers reach the optimization through the shared direct-read query';

CREATE TABLE tab_array (
	id UInt64,
	arr Array(String),
	INDEX idx arr TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_array SELECT number, if(number % 2 = 0, ['foo', 'bar'], ['baz']) FROM numbers(1000);

SELECT '-- fires: has (single token)';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_array WHERE has(arr, 'foo')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab_array WHERE has(arr, 'foo');
SELECT count() FROM tab_array WHERE has(arr, 'foo') SETTINGS query_plan_optimize_count_from_text_index = 0;

SELECT '-- fires: hasAny (union)';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_array WHERE hasAny(arr, ['foo', 'baz'])) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab_array WHERE hasAny(arr, ['foo', 'baz']);
SELECT count() FROM tab_array WHERE hasAny(arr, ['foo', 'baz']) SETTINGS query_plan_optimize_count_from_text_index = 0;

SELECT '-- fires: hasAll (intersection)';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_array WHERE hasAll(arr, ['foo', 'bar'])) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab_array WHERE hasAll(arr, ['foo', 'bar']);
SELECT count() FROM tab_array WHERE hasAll(arr, ['foo', 'bar']) SETTINGS query_plan_optimize_count_from_text_index = 0;

DROP TABLE tab_array;

CREATE TABLE tab_map (
	id UInt64,
	m Map(String, String),
	INDEX idx_keys mapKeys(m) TYPE text(tokenizer = array),
	INDEX idx_values mapValues(m) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_map SELECT number, if(number % 2 = 0, map('k0', 'v0', 'k1', 'v1'), map('k2', 'v2')) FROM numbers(1000);

SELECT '-- fires: mapContains (key)';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_map WHERE mapContains(m, 'k0')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab_map WHERE mapContains(m, 'k0');
SELECT count() FROM tab_map WHERE mapContains(m, 'k0') SETTINGS query_plan_optimize_count_from_text_index = 0;

SELECT '-- fires: mapContainsValue';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_map WHERE mapContainsValue(m, 'v0')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab_map WHERE mapContainsValue(m, 'v0');
SELECT count() FROM tab_map WHERE mapContainsValue(m, 'v0') SETTINGS query_plan_optimize_count_from_text_index = 0;

SELECT '-- does not fire: m[k] = v needs a residual recheck (hint mode, not exact)';
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab_map WHERE m['k0'] = 'v0') WHERE explain LIKE '%Trivial count from text index%';
SELECT count() FROM tab_map WHERE m['k0'] = 'v0';

DROP TABLE tab_map;

SELECT 'Partially materialized text index';

CREATE TABLE tab_partial (
	id UInt64,
	text String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_partial SELECT number, if(number % 2 = 0, 'alpha beta', 'gamma delta') FROM numbers(1000);

ALTER TABLE tab_partial ADD INDEX idx text TYPE text(tokenizer = splitByNonAlpha);

SYSTEM STOP MERGES tab_partial;

INSERT INTO tab_partial SELECT number, if(number % 4 = 0, 'alpha epsilon', 'zeta') FROM numbers(1000);

SELECT '-- one part without the index, one part with it';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partial' AND active AND secondary_indices_marks_bytes = 0;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partial' AND active AND secondary_indices_marks_bytes > 0;

SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_partial WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%AggregatingProjection%' OR explain LIKE '%ReadFromTextIndexCount%';

SELECT '-- results match the reader';
SELECT count() FROM tab_partial WHERE hasToken(text, 'alpha') SETTINGS log_comment = 'trivial_count_partial_on';
SELECT count() FROM tab_partial WHERE hasToken(text, 'alpha') SETTINGS query_plan_optimize_count_from_text_index = 0, log_comment = 'trivial_count_partial_off';
SELECT count() FROM tab_partial WHERE hasAnyTokens(text, ['alpha', 'zeta']);
SELECT count() FROM tab_partial WHERE hasAnyTokens(text, ['alpha', 'zeta']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab_partial WHERE hasAllTokens(text, ['alpha', 'beta']);
SELECT count() FROM tab_partial WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT count() FROM tab_partial WHERE hasToken(text, 'missing');
SELECT count() FROM tab_partial WHERE hasToken(text, 'missing') SETTINGS query_plan_optimize_count_from_text_index = 0;

SYSTEM FLUSH LOGS query_log;
SELECT '-- the optimization reads fewer rows: only the unindexed part';
SELECT (SELECT sum(read_rows) FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 120 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'trivial_count_partial_on')
     < (SELECT sum(read_rows) FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 120 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'trivial_count_partial_off');

SELECT '-- fully materialized after ALTER: back to the plain count source';
SYSTEM START MERGES tab_partial;

ALTER TABLE tab_partial MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partial' AND active AND secondary_indices_marks_bytes = 0;
SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab_partial WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%AggregatingProjection%';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_partial WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab_partial WHERE hasToken(text, 'alpha');

SELECT '-- does not fire: no part has the index';
CREATE TABLE tab_unindexed (
	id UInt64,
	text String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_unindexed SELECT number, 'alpha' FROM numbers(100);

ALTER TABLE tab_unindexed ADD INDEX idx text TYPE text(tokenizer = splitByNonAlpha);

SELECT count(explain) FROM (EXPLAIN SELECT count() FROM tab_unindexed WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%Trivial count from text index%';
SELECT count() FROM tab_unindexed WHERE hasToken(text, 'alpha');

DROP TABLE tab_partial;
DROP TABLE tab_unindexed;
