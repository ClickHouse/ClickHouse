-- Tags: no-fasttest
-- no-fasttest: needs the JSON type and the text (full-text) index.

-- A metadata-only / lazy `ALTER MODIFY COLUMN` (here a lazy JSON type hint) leaves an old part's
-- path stored in its on-disk representation (Dynamic) while metadata already says the hinted type
-- (Nullable(String)). Reading that column through the `text` index code paths must still apply the
-- read-time alter conversion. Before the fix the on-disk Dynamic column reached `materialize` in
-- `evaluateMissingDefaults` mislabeled as Nullable(String) and threw
-- `Unexpected return type from materialize`.

SET allow_experimental_json_lazy_type_hints = 1;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_text_lazy_alter;

CREATE TABLE t_text_lazy_alter (id UInt64, doc JSON) ENGINE = MergeTree ORDER BY id;

-- Part written before the hint/index: doc.a.id is stored as Dynamic and has no index granule.
INSERT INTO t_text_lazy_alter SELECT number, '{"a":{"id":"actor' || toString(number % 7) || '"}}' FROM numbers(4000);

-- Lazy type hint: metadata-only, the existing part is not rewritten.
ALTER TABLE t_text_lazy_alter MODIFY COLUMN doc JSON(`a.id` Nullable(String));

-- Text index with the `array` tokenizer -> exact-equals direct read (drops the residual equals).
ALTER TABLE t_text_lazy_alter ADD INDEX idx doc.a.id TYPE text(tokenizer = array) GRANULARITY 100000000;

-- Part written after the index: it has the granule, so the optimizer picks direct read.
INSERT INTO t_text_lazy_alter SELECT number + 500000, '{"a":{"id":"actor' || toString(number % 7) || '"}}' FROM numbers(1000);

SELECT '-- exact equals via text-index direct read (computed on the no-granule part from data)';
SELECT count() FROM t_text_lazy_alter WHERE doc.a.id = 'actor3'
    SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- LIKE via the text-index fallback path';
SELECT count() FROM t_text_lazy_alter WHERE doc.a.id LIKE '%actor3%'
    SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, text_index_like_min_pattern_length = 1, text_index_like_max_postings_to_read = 1;

SELECT '-- control: the same answer without the skip index';
SELECT count() FROM t_text_lazy_alter WHERE doc.a.id = 'actor3' SETTINGS use_skip_indexes = 0;

DROP TABLE t_text_lazy_alter;

-- Not JSON-specific: a regular `ALTER MODIFY COLUMN` with an unfinished mutation (interrupted by
-- KILL MUTATION, so the on-disk String stays under the new Nullable(String) type) hits the same path.
DROP TABLE IF EXISTS t_text_regular_alter;

CREATE TABLE t_text_regular_alter (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_text_regular_alter SELECT number, 'actor' || toString(number % 7) FROM numbers(4000);

SYSTEM STOP MERGES t_text_regular_alter;
ALTER TABLE t_text_regular_alter MODIFY COLUMN s Nullable(String) SETTINGS mutations_sync = 0, alter_sync = 0;
KILL MUTATION WHERE table = 't_text_regular_alter' AND database = currentDatabase() FORMAT Null;
ALTER TABLE t_text_regular_alter ADD INDEX idx s TYPE text(tokenizer = array) GRANULARITY 100000000 SETTINGS mutations_sync = 0, alter_sync = 0;
INSERT INTO t_text_regular_alter SELECT number + 500000, 'actor' || toString(number % 7) FROM numbers(1000);

SELECT '-- regular ALTER MODIFY with an interrupted mutation, read via text index';
SELECT count() FROM t_text_regular_alter WHERE s = 'actor3'
    SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_regular_alter;
