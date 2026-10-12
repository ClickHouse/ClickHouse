-- A text index with `scoring = 'bm25'` must stay loadable after the table settings it was validated against change:
-- the experimental gate is checked only on DDL, and the posting list codec must be a part of the index definition.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_scoring_attach;
DROP TABLE IF EXISTS tab_scoring_no_codec;

SELECT '-- the codec of a scoring index is not taken from the table setting';
CREATE TABLE tab_scoring_no_codec
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', scoring = 'bm25')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS text_index_posting_list_codec = 'bitpacking', allow_experimental_text_index_scoring = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab_scoring_no_codec (id UInt32, body String)
ENGINE = MergeTree
ORDER BY id
SETTINGS text_index_posting_list_codec = 'bitpacking', allow_experimental_text_index_scoring = 1;

ALTER TABLE tab_scoring_no_codec ADD INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', scoring = 'bm25'); -- { serverError BAD_ARGUMENTS }

SELECT '-- the index stays loadable after the gate is turned off and the default codec changes';
CREATE TABLE tab_scoring_attach
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1;

INSERT INTO tab_scoring_attach VALUES (1, 'raft consensus raft'), (2, 'stream processing'), (3, 'raft log');

ALTER TABLE tab_scoring_attach MODIFY SETTING allow_experimental_text_index_scoring = 0, text_index_posting_list_codec = 'none';

DETACH TABLE tab_scoring_attach;
ATTACH TABLE tab_scoring_attach;

INSERT INTO tab_scoring_attach VALUES (4, 'raft raft raft');

SELECT id FROM tab_scoring_attach WHERE hasToken(body, 'raft') ORDER BY bm25() DESC, id;

DROP TABLE tab_scoring_attach;
DROP TABLE tab_scoring_no_codec;
