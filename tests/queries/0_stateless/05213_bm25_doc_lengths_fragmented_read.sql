-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_doc_lengths;

CREATE TABLE tab_doc_lengths
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, allow_experimental_text_index_scoring = 1;

SYSTEM STOP MERGES tab_doc_lengths;

-- Two parts. The document length depends on the row, so a document length taken from a wrong row changes the score.
INSERT INTO tab_doc_lengths
SELECT number, concat(if(number % 7 = 0, 'raft ', ''), arrayStringConcat(arrayMap(x -> 'filler', range(number % 13)), ' '))
FROM numbers(5000);

INSERT INTO tab_doc_lengths
SELECT number, concat(if(number % 7 = 0, 'raft ', ''), arrayStringConcat(arrayMap(x -> 'filler', range(number % 13)), ' '))
FROM numbers(5000, 3000);

SELECT '-- scores of scattered primary key ranges';
SELECT id, round(_bm25_score, 4)
FROM tab_doc_lengths
WHERE hasAnyTokens(body, 'raft') AND (id BETWEEN 100 AND 130 OR id BETWEEN 4990 AND 5010 OR id BETWEEN 7980 AND 7999)
ORDER BY id;

SELECT '-- the fragmented read gives the same scores as the sequential read of the whole table';
SELECT countIf(fragmented.s != full.s) AS mismatches, count() AS rows
FROM
(
    SELECT id, round(_bm25_score, 5) AS s
    FROM tab_doc_lengths
    WHERE hasAnyTokens(body, 'raft') AND (id BETWEEN 100 AND 130 OR id BETWEEN 1000 AND 1200 OR id BETWEEN 4990 AND 5010 OR id BETWEEN 7900 AND 7999)
) AS fragmented
INNER JOIN
(
    SELECT id, round(_bm25_score, 5) AS s FROM tab_doc_lengths WHERE hasAnyTokens(body, 'raft')
) AS full USING (id);

SELECT '-- the same with blocks smaller than a granule, so reads resume in the middle of granules';
SELECT countIf(fragmented.s != full.s) AS mismatches, count() AS rows
FROM
(
    SELECT id, round(_bm25_score, 5) AS s
    FROM tab_doc_lengths
    WHERE hasAnyTokens(body, 'raft') AND (id BETWEEN 100 AND 130 OR id BETWEEN 1000 AND 1200 OR id BETWEEN 4990 AND 5010 OR id BETWEEN 7900 AND 7999)
    SETTINGS max_block_size = 50, max_threads = 1
) AS fragmented
INNER JOIN
(
    SELECT id, round(_bm25_score, 5) AS s FROM tab_doc_lengths WHERE hasAnyTokens(body, 'raft') SETTINGS max_block_size = 50, max_threads = 1
) AS full USING (id);

SELECT '-- the same after merging the parts, which rewrites the index';
SYSTEM START MERGES tab_doc_lengths;
OPTIMIZE TABLE tab_doc_lengths FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_doc_lengths' AND active;

SELECT countIf(fragmented.s != full.s) AS mismatches, count() AS rows
FROM
(
    SELECT id, round(_bm25_score, 5) AS s
    FROM tab_doc_lengths
    WHERE hasAnyTokens(body, 'raft') AND (id BETWEEN 100 AND 130 OR id BETWEEN 1000 AND 1200 OR id BETWEEN 4990 AND 5010 OR id BETWEEN 7900 AND 7999)
    SETTINGS max_block_size = 50, max_threads = 1
) AS fragmented
INNER JOIN
(
    SELECT id, round(_bm25_score, 5) AS s FROM tab_doc_lengths WHERE hasAnyTokens(body, 'raft')
) AS full USING (id);

SELECT id, round(_bm25_score, 4)
FROM tab_doc_lengths
WHERE hasAnyTokens(body, 'raft') AND (id BETWEEN 100 AND 130 OR id BETWEEN 4990 AND 5010 OR id BETWEEN 7980 AND 7999)
ORDER BY id;

DROP TABLE tab_doc_lengths;
