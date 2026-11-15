-- https://github.com/ClickHouse/ClickHouse/issues/116832
-- A direct read from a text index rewrites the search predicate into a column synthesized by the
-- index reader, named with the `__text_index_` prefix. With `apply_mutations_on_fly` and a pending
-- mutation whose stage has to carry the query's read columns, that name was handed to the mutation
-- interpreter, which resolves names against the storage and threw `NO_SUCH_COLUMN_IN_TABLE` - so no
-- `hasToken` query could run at all until the mutation materialized.

SET apply_mutations_on_fly = 1;
-- The runner randomizes this setting off in a fraction of the runs, which would turn every arm below
-- into a copy of the `query_plan_direct_read_from_text_index = 0` arm and hide the regression.
SET query_plan_direct_read_from_text_index = 1;

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

-- `DELETE` is not the only trigger: a pending `ALTER UPDATE` of a column in the query's read set is
-- kept by `AlterConversions::filterMutationCommands` and its stage carries the read columns too, so
-- it threw the very same exception. (An `UPDATE` of the indexed column itself is a different story:
-- there `canUseIndex` disables the direct read altogether, which is what makes dropping the
-- synthesized name safe - it can only appear while the index is actually used.)
SELECT 'pending update';

DROP TABLE IF EXISTS t_text_index_on_fly_update;
CREATE TABLE t_text_index_on_fly_update (id UInt64, c UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_index_on_fly_update SELECT number, 0, concat('tok', toString(number % 10), ' word') FROM numbers(1000);

SYSTEM STOP MERGES t_text_index_on_fly_update;
ALTER TABLE t_text_index_on_fly_update UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 0;

SELECT sum(c) FROM t_text_index_on_fly_update WHERE hasToken(s, 'tok1');

DROP TABLE t_text_index_on_fly_update;

-- The `__text_index_` prefix is not reserved, so a table may declare a column of its own with such a
-- name, in a table that has no text index at all. Filtering it out of the mutation input by the name
-- pattern alone would hide it from `filterMutationCommands`, which would then discard the pending
-- `ALTER UPDATE` below and answer from the stale on-disk values.
SELECT 'shadowing column';

DROP TABLE IF EXISTS t_text_index_shadow;
CREATE TABLE t_text_index_shadow (id UInt64, __text_index_payload UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_index_shadow SELECT number, 0 FROM numbers(100);

SYSTEM STOP MERGES t_text_index_shadow;
ALTER TABLE t_text_index_shadow UPDATE __text_index_payload = 777 WHERE id < 10 SETTINGS mutations_sync = 0;

SELECT max(__text_index_payload) FROM t_text_index_shadow WHERE id < 10;

DROP TABLE t_text_index_shadow;
