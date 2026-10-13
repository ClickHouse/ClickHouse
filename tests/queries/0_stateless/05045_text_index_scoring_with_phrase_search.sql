-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_phrase_scoring;
DROP TABLE IF EXISTS tab_phrase_scoring_ref;

-- An index with both positions (`.pos`) and scoring (`.dl`) substreams: the part reports text-index
-- format version 3, and phrase search must still find and read the positions substream.
CREATE TABLE tab_phrase_scoring
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', support_phrase_search = 1, scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_phrase_search = 1, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_phrase_scoring VALUES
    (1, 'raft consensus log'),
    (2, 'consensus raft log'),
    (3, 'raft log consensus'),
    (4, 'stream processing pipeline'),
    (5, 'raft consensus protocol overview');

SELECT '-- hasPhrase uses the positions of the scoring index';
SELECT groupArray(id) FROM tab_phrase_scoring WHERE hasPhrase(body, 'raft consensus');
SELECT groupArray(id) FROM tab_phrase_scoring WHERE hasPhrase(body, 'consensus raft');
SELECT groupArray(id) FROM tab_phrase_scoring WHERE hasPhrase(body, 'raft protocol');

SELECT '-- the same after a merge of multiple parts';
INSERT INTO tab_phrase_scoring VALUES (6, 'raft consensus basics'), (7, 'log raft');
OPTIMIZE TABLE tab_phrase_scoring FINAL;
SELECT groupArray(id) FROM tab_phrase_scoring WHERE hasPhrase(body, 'raft consensus');

-- The reference: the same rows inserted at once, so its index is built without a merge.
CREATE TABLE tab_phrase_scoring_ref
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', support_phrase_search = 1, scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_phrase_search = 1, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_phrase_scoring_ref SELECT * FROM tab_phrase_scoring;

SELECT '-- bm25() after the merge matches the index built without a merge';
SELECT
    merged.id,
    round(merged.score, 4),
    if(abs(merged.score - ref.score) <= 1e-6, 'OK', format('MISMATCH {} vs {}', merged.score, ref.score))
FROM (SELECT id, bm25() AS score FROM tab_phrase_scoring WHERE hasToken(body, 'consensus')) AS merged
INNER JOIN (SELECT id, bm25() AS score FROM tab_phrase_scoring_ref WHERE hasToken(body, 'consensus')) AS ref ON merged.id = ref.id
ORDER BY merged.id;

SELECT '-- hasPhrase filters, hasToken provides the scoring tokens';
SELECT
    merged.id,
    round(merged.score, 4),
    if(abs(merged.score - ref.score) <= 1e-6, 'OK', format('MISMATCH {} vs {}', merged.score, ref.score))
FROM (SELECT id, bm25() AS score FROM tab_phrase_scoring WHERE hasToken(body, 'log') AND hasPhrase(body, 'raft consensus')) AS merged
INNER JOIN (SELECT id, bm25() AS score FROM tab_phrase_scoring_ref WHERE hasToken(body, 'log') AND hasPhrase(body, 'raft consensus')) AS ref ON merged.id = ref.id
ORDER BY merged.id;

DROP TABLE tab_phrase_scoring;
DROP TABLE tab_phrase_scoring_ref;
