-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_bm25_err;
DROP TABLE IF EXISTS tab_bm25_no_scoring;
DROP TABLE IF EXISTS tab_bm25_two_indexes;
DROP TABLE IF EXISTS tab_bm25_shadow;
DROP TABLE IF EXISTS tab_bm25_unmaterialized;
DROP TABLE IF EXISTS tab_bm25_no_index;

SELECT '-- enable_scoring requires the experimental MergeTree setting';
CREATE TABLE tab_bm25_err
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE tab_bm25_err
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_err VALUES (1, 'raft consensus log'), (2, 'stream processing');

SELECT '-- the setting is off';
SELECT id, _bm25_score FROM tab_bm25_err WHERE hasToken(body, 'raft')
SETTINGS allow_experimental_bm25_score_column = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- direct read from the text index is off';
SELECT id, _bm25_score FROM tab_bm25_err WHERE hasToken(body, 'raft')
SETTINGS query_plan_direct_read_from_text_index = 0; -- { serverError BAD_ARGUMENTS }

SELECT '-- no text-search predicate at all';
SELECT id, _bm25_score FROM tab_bm25_err WHERE id > 0; -- { serverError BAD_ARGUMENTS }

SELECT '-- no WHERE at all';
SELECT id, _bm25_score FROM tab_bm25_err; -- { serverError BAD_ARGUMENTS }

SELECT '-- only predicates outside the three scoring functions (they filter but do not score)';
SELECT id, _bm25_score FROM tab_bm25_err WHERE body = 'raft consensus log'; -- { serverError BAD_ARGUMENTS }

SELECT '-- the index has no enable_scoring';
CREATE TABLE tab_bm25_no_scoring
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_bm25_no_scoring VALUES (1, 'raft consensus log');

SELECT id, _bm25_score FROM tab_bm25_no_scoring WHERE hasToken(body, 'raft'); -- { serverError BAD_ARGUMENTS }

SELECT '-- predicates on two scoring indexes are not supported';
CREATE TABLE tab_bm25_two_indexes
(
    id UInt32,
    title String,
    body String,
    INDEX idx_title(title) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_two_indexes VALUES (1, 'raft', 'consensus log');

SELECT id, _bm25_score FROM tab_bm25_two_indexes
WHERE hasToken(title, 'raft') AND hasToken(body, 'consensus'); -- { serverError BAD_ARGUMENTS }

SELECT '-- also when one of the two scoring predicates stays row-wise (not direct-read)';
SELECT id, _bm25_score FROM tab_bm25_two_indexes
WHERE hasToken(title, 'raft') AND (hasToken(body, 'consensus') OR id = 1); -- { serverError BAD_ARGUMENTS }

SELECT '-- a single scoring index among two still works';
SELECT id, _bm25_score > 0 FROM tab_bm25_two_indexes WHERE hasToken(title, 'raft');

SELECT '-- a physical column named _bm25_score shadows the virtual one';
CREATE TABLE tab_bm25_shadow
(
    id UInt32,
    body String,
    _bm25_score Float32,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_shadow VALUES (1, 'raft consensus log', 42.5);

SELECT id, _bm25_score FROM tab_bm25_shadow WHERE hasToken(body, 'raft');

SELECT '-- a part without the materialized index asks for MATERIALIZE INDEX';
CREATE TABLE tab_bm25_unmaterialized
(
    id UInt32,
    body String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_unmaterialized VALUES (1, 'raft consensus log');
ALTER TABLE tab_bm25_unmaterialized ADD INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1;
INSERT INTO tab_bm25_unmaterialized VALUES (2, 'raft quorum');

SELECT id, _bm25_score FROM tab_bm25_unmaterialized WHERE hasToken(body, 'raft'); -- { serverError BAD_ARGUMENTS }

ALTER TABLE tab_bm25_unmaterialized MATERIALIZE INDEX idx_body SETTINGS mutations_sync = 2;

SELECT id, _bm25_score > 0 FROM tab_bm25_unmaterialized WHERE hasToken(body, 'raft') ORDER BY id;

DROP TABLE tab_bm25_err;
DROP TABLE tab_bm25_no_scoring;
DROP TABLE tab_bm25_two_indexes;
DROP TABLE tab_bm25_shadow;
DROP TABLE tab_bm25_unmaterialized;

SELECT '-- a table without any text index rejects the column too';
CREATE TABLE tab_bm25_no_index (id UInt32, body String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_bm25_no_index VALUES (1, 'raft');
SELECT id, _bm25_score FROM tab_bm25_no_index; -- { serverError BAD_ARGUMENTS }

SELECT '-- unless asterisk_include_virtual_columns pulls it in (kept zero-filled)';
SELECT id, _bm25_score FROM tab_bm25_no_index SETTINGS asterisk_include_virtual_columns = 1;

DROP TABLE tab_bm25_no_index;
