-- While a `DROP COLUMN` followed by a `RENAME COLUMN` to the dropped name, or by re-adding a column
-- with the same name, is still pending, reads already apply it, but the text index file of a part
-- is found by the index name. If the index is re-created over the new column in the same `ALTER`,
-- the file found that way was built over the dropped column's data, so a direct read from it would
-- answer the search predicate from the old data. Each query is compared against a row scan.

-- The runner randomizes this setting off in a fraction of the runs, which would turn every arm below
-- into a copy of the row-scan control and hide the regression.
SET query_plan_direct_read_from_text_index = 1;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_text_index_pending_rename;
CREATE TABLE t_text_index_pending_rename (id Int64, s String, u String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id;
-- Only to hold the mutation pending deterministically.
SYSTEM STOP MERGES t_text_index_pending_rename;
INSERT INTO t_text_index_pending_rename VALUES (1, 'apple', 'banana'), (2, 'cherry', 'apple');

ALTER TABLE t_text_index_pending_rename DROP INDEX idx, DROP COLUMN s, RENAME COLUMN u TO s, ADD INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT groupArray(s) FROM (SELECT s FROM t_text_index_pending_rename ORDER BY id);
SELECT 'banana', groupArray(id), (SELECT groupArray(id) FROM t_text_index_pending_rename WHERE hasToken(s, 'banana') SETTINGS use_skip_indexes = 0)
FROM t_text_index_pending_rename WHERE hasToken(s, 'banana');
SELECT 'apple', groupArray(id), (SELECT groupArray(id) FROM t_text_index_pending_rename WHERE hasToken(s, 'apple') SETTINGS use_skip_indexes = 0)
FROM t_text_index_pending_rename WHERE hasToken(s, 'apple');
SELECT 'cherry', groupArray(id), (SELECT groupArray(id) FROM t_text_index_pending_rename WHERE hasToken(s, 'cherry') SETTINGS use_skip_indexes = 0)
FROM t_text_index_pending_rename WHERE hasToken(s, 'cherry');
SELECT 'count apple', count(), (SELECT count() FROM t_text_index_pending_rename WHERE hasToken(s, 'apple') SETTINGS use_skip_indexes = 0)
FROM t_text_index_pending_rename WHERE hasToken(s, 'apple');

SYSTEM START MERGES t_text_index_pending_rename;
DROP TABLE t_text_index_pending_rename;

DROP TABLE IF EXISTS t_text_index_pending_readd;
CREATE TABLE t_text_index_pending_readd (id Int64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_text_index_pending_readd;
INSERT INTO t_text_index_pending_readd VALUES (1, 'apple'), (2, 'cherry');

ALTER TABLE t_text_index_pending_readd DROP INDEX idx, DROP COLUMN s, ADD COLUMN s String DEFAULT 'banana', ADD INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT groupArray(s) FROM (SELECT s FROM t_text_index_pending_readd ORDER BY id);
SELECT 'banana', groupArray(id), (SELECT groupArray(id) FROM t_text_index_pending_readd WHERE hasToken(s, 'banana') SETTINGS use_skip_indexes = 0)
FROM t_text_index_pending_readd WHERE hasToken(s, 'banana');
SELECT 'apple', groupArray(id), (SELECT groupArray(id) FROM t_text_index_pending_readd WHERE hasToken(s, 'apple') SETTINGS use_skip_indexes = 0)
FROM t_text_index_pending_readd WHERE hasToken(s, 'apple');
SELECT 'count banana', count(), (SELECT count() FROM t_text_index_pending_readd WHERE hasToken(s, 'banana') SETTINGS use_skip_indexes = 0)
FROM t_text_index_pending_readd WHERE hasToken(s, 'banana');

SYSTEM START MERGES t_text_index_pending_readd;
DROP TABLE t_text_index_pending_readd;
