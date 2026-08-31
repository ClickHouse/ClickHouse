-- https://github.com/ClickHouse/ClickHouse/issues/116832
-- A direct read from a text index rewrites the search predicate into an internal virtual column that
-- the index reader produces. With `apply_mutations_on_fly` and a pending `ALTER DELETE`, that name was
-- handed to the mutation interpreter, which resolves names against the storage and threw
-- `NO_SUCH_COLUMN_IN_TABLE` - so no `hasToken` query could run at all until the mutation materialized.

SET apply_mutations_on_fly = 1;

DROP TABLE IF EXISTS t_text_index_on_fly;
CREATE TABLE t_text_index_on_fly (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_index_on_fly SELECT number, concat('tok', toString(number % 10), ' word') FROM numbers(1000);

SYSTEM STOP MERGES t_text_index_on_fly;
ALTER TABLE t_text_index_on_fly DELETE WHERE id < 100 SETTINGS mutations_sync = 0;

SELECT count() FROM t_text_index_on_fly WHERE hasToken(s, 'tok1');
SELECT count() FROM t_text_index_on_fly WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_text_index_on_fly WHERE hasToken(s, 'tok1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_on_fly WHERE hasToken(s, 'tok1') SETTINGS apply_mutations_on_fly = 0;

SELECT count() FROM t_text_index_on_fly WHERE hasAnyTokens(s, ['tok1', 'tok2']);
SELECT count() FROM t_text_index_on_fly WHERE hasAnyTokens(s, ['tok1', 'tok2']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_text_index_on_fly WHERE hasAllTokens(s, ['tok1', 'word']);
SELECT count() FROM t_text_index_on_fly WHERE hasAllTokens(s, ['tok1', 'word']) SETTINGS query_plan_direct_read_from_text_index = 0;

-- And the same after the mutation materializes.
SELECT 'materialized';
SYSTEM START MERGES t_text_index_on_fly;
ALTER TABLE t_text_index_on_fly DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT count() FROM t_text_index_on_fly WHERE hasToken(s, 'tok1');

DROP TABLE t_text_index_on_fly;
