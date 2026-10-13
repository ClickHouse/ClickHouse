-- Tags: no-parallel-replicas

-- A text index over `LowCardinality(String)` tokenizes each dictionary value once and reuses its tokens for later rows
-- with the same value. The document length of such a row must count the reused tokens, so the scores match the ones
-- of the same data stored as `String`.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_string;
DROP TABLE IF EXISTS tab_low_cardinality;

CREATE TABLE tab_string
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, allow_experimental_text_index_scoring = 1;

CREATE TABLE tab_low_cardinality
(
    id UInt32,
    body LowCardinality(String),
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, allow_experimental_text_index_scoring = 1;

-- 26 distinct values of different lengths over 2000 rows, inserted as one block, so each value repeats many times.
INSERT INTO tab_string
SELECT number, concat(if(number % 7 = 0, 'raft ', ''), arrayStringConcat(arrayMap(x -> 'filler', range(number % 13)), ' '))
FROM numbers(2000)
SETTINGS max_block_size = 65536, min_insert_block_size_rows = 65536, min_insert_block_size_bytes = 0;

INSERT INTO tab_low_cardinality
SELECT number, concat(if(number % 7 = 0, 'raft ', ''), arrayStringConcat(arrayMap(x -> 'filler', range(number % 13)), ' '))
FROM numbers(2000)
SETTINGS max_block_size = 65536, min_insert_block_size_rows = 65536, min_insert_block_size_bytes = 0;

SELECT '-- scores of the first and of a repeated occurrence of the same values';
SELECT id, body, round(bm25(), 4)
FROM tab_low_cardinality
WHERE hasAnyTokens(body, 'raft') AND id IN (0, 7, 14, 91, 98, 105)
ORDER BY id;

SELECT '-- the same scores as for String';
SELECT countIf(s.score != lc.score) AS mismatches, count() AS rows
FROM
(
    SELECT id, round(bm25(), 5) AS score FROM tab_string WHERE hasAnyTokens(body, 'raft')
) AS s
INNER JOIN
(
    SELECT id, round(bm25(), 5) AS score FROM tab_low_cardinality WHERE hasAnyTokens(body, 'raft')
) AS lc USING (id);

DROP TABLE tab_string;
DROP TABLE tab_low_cardinality;
