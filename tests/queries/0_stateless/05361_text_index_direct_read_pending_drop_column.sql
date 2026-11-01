-- A pending `DROP COLUMN` is a metadata mutation, and a column with the dropped name can be added again
-- right away. The part still holds the text index file built over the dropped column, so a direct read
-- from the text index must not answer a search on the re-added column from those stale postings.

SET apply_mutations_on_fly = 1;
-- The runner randomizes this setting off in a fraction of the runs, which would hide the regression.
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS t_text_index_drop;
CREATE TABLE t_text_index_drop (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_text_index_drop;
INSERT INTO t_text_index_drop SELECT number, concat('old', toString(number % 10), ' word') FROM numbers(100);

ALTER TABLE t_text_index_drop DROP INDEX idx SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_text_index_drop DROP COLUMN s SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_text_index_drop ADD COLUMN s String DEFAULT concat('new', toString(id % 10), ' word');
ALTER TABLE t_text_index_drop ADD INDEX idx s TYPE text(tokenizer = splitByNonAlpha);

SELECT 'the drop is pending', count() > 0 FROM system.mutations
WHERE database = currentDatabase() AND table = 't_text_index_drop' AND NOT is_done;

SELECT 'a token of the re-added column', count() FROM t_text_index_drop WHERE hasToken(s, 'new1');
SELECT 'a token of the re-added column, without the index', count() FROM t_text_index_drop WHERE hasToken(s, 'new1') SETTINGS use_skip_indexes = 0;
SELECT 'a token of the dropped column', count() FROM t_text_index_drop WHERE hasToken(s, 'old1');
SELECT 'any token', count() FROM t_text_index_drop WHERE hasAnyTokens(s, ['new1', 'old2']);
SELECT 'all tokens', count() FROM t_text_index_drop WHERE hasAllTokens(s, ['new1', 'word']);

DROP TABLE t_text_index_drop;
