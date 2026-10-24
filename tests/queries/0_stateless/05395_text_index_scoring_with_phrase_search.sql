-- An index with both positions (`.pos`) and scoring (`.dl`) substreams: the part reports text-index
-- format version 3, and phrase search must still find and read the positions substream.

SET enable_analyzer = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab_phrase_scoring;

CREATE TABLE tab_phrase_scoring
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', support_phrase_search = 1, scoring = 'bm25')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_phrase_search = 1, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_phrase_scoring VALUES
    (1, 'raft consensus log'),
    (2, 'consensus raft log'),
    (3, 'raft log consensus'),
    (4, 'stream processing pipeline'),
    (5, 'raft consensus protocol overview raft');

SELECT '-- hasPhrase uses the positions of the scoring index';
SELECT groupArray(id) FROM tab_phrase_scoring WHERE hasPhrase(body, 'raft consensus');
SELECT groupArray(id) FROM tab_phrase_scoring WHERE hasPhrase(body, 'consensus raft');
SELECT groupArray(id) FROM tab_phrase_scoring WHERE hasPhrase(body, 'raft protocol');

SELECT '-- the same after a merge of multiple parts';
INSERT INTO tab_phrase_scoring VALUES (6, 'raft consensus basics'), (7, 'log raft');
OPTIMIZE TABLE tab_phrase_scoring FINAL;
SELECT groupArray(id) FROM tab_phrase_scoring WHERE hasPhrase(body, 'raft consensus');

SELECT '-- the weights and the positions of the merged part';
SELECT row_id, weight, positions FROM mergeTreeTextIndex(currentDatabase(), tab_phrase_scoring, idx_body) WHERE token = 'raft' ORDER BY row_id;

DROP TABLE tab_phrase_scoring;
