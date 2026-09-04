-- Tags: no-parallel-replicas, no-replicated-database
-- The direct text index read rewrite (query_plan_direct_read_from_text_index = 1) replaces a
-- hasToken predicate over the indexed column with a lookup into the part's on-disk text index.
-- A pending ALTER that drops or renames the indexed column leaves that index stale: the index
-- files under the old name describe data the column no longer returns, so trusting them silently
-- drops (or fabricates) matching rows. canUseIndex must fail the direct-read rewrite open for
-- stale parts so the query falls back to the row-level predicate and returns the same rows as
-- query_plan_direct_read_from_text_index = 0.
--
-- MODIFY COLUMN is included as a regression guard (it already fails open via
-- all_updated_columns), while DROP+ADD and DROP+RENAME only fail open through the
-- dropped/renamed checks this PR adds to canUseIndex.

SET enable_analyzer = 1;
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

-- Pending MODIFY COLUMN on the text-indexed column: reads return the same rows, and the direct
-- read must stay suppressed for the part whose index was built with the old column layout.
DROP TABLE IF EXISTS text_modify;
CREATE TABLE text_modify (id UInt32, s String, INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES text_modify;
INSERT INTO text_modify SELECT number, 'foo needle bar' FROM numbers(50000) SETTINGS max_insert_threads = 1;
INSERT INTO text_modify VALUES (50000, 'zzz');
ALTER TABLE text_modify MODIFY COLUMN s LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'modify direct=0', count() FROM text_modify WHERE hasToken(s, 'needle') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'modify direct=1', count() FROM text_modify WHERE hasToken(s, 'needle') SETTINGS query_plan_direct_read_from_text_index = 1;
DROP TABLE text_modify;

-- Pending DROP COLUMN + ADD COLUMN with a DEFAULT: reads return the new default ('baz') for every
-- row, but each part still carries a text index built from the old data. The index for part 1
-- advertises 'needle' granules that no longer exist, so a direct read would fabricate matches.
DROP TABLE IF EXISTS text_drop_add;
CREATE TABLE text_drop_add (id UInt32, s String, INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES text_drop_add;
INSERT INTO text_drop_add SELECT number, 'foo needle bar' FROM numbers(50000) SETTINGS max_insert_threads = 1;
INSERT INTO text_drop_add VALUES (50000, 'zzz');
ALTER TABLE text_drop_add (DROP COLUMN s), (ADD COLUMN s String DEFAULT 'baz') SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'drop-add direct=0', count() FROM text_drop_add WHERE hasToken(s, 'needle') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'drop-add direct=1', count() FROM text_drop_add WHERE hasToken(s, 'needle') SETTINGS query_plan_direct_read_from_text_index = 1;
DROP TABLE text_drop_add;

-- Pending DROP COLUMN + RENAME COLUMN into the freed name, written as two separate commands.
-- Reads of every part return the renamed r column's data ('needle ...'), but the text index files
-- under the name s still describe the old s column ('alpha beta' / 'zzz'), which has no 'needle'
-- tokens: a direct read would silently return 0 instead of 50001.
DROP TABLE IF EXISTS text_drop_rename;
CREATE TABLE text_drop_rename (id UInt32, s String, r String, INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES text_drop_rename;
INSERT INTO text_drop_rename SELECT number, 'alpha beta', 'needle bar' FROM numbers(50000) SETTINGS max_insert_threads = 1;
INSERT INTO text_drop_rename VALUES (50000, 'zzz', 'needle here');
ALTER TABLE text_drop_rename (DROP COLUMN s), (RENAME COLUMN r TO s) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'drop-rename direct=0', count() FROM text_drop_rename WHERE hasToken(s, 'needle') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'drop-rename direct=1', count() FROM text_drop_rename WHERE hasToken(s, 'needle') SETTINGS query_plan_direct_read_from_text_index = 1;
DROP TABLE text_drop_rename;
