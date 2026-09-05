-- Tags: no-parallel-replicas

-- The materialization of a text index writes temporary segments (bounded by `text_index_max_processed_tokens_before_flush`)
-- and merges them into the final index. With BM25 scoring, each segment holds the document lengths of its own rows
-- only, while its row ids stay absolute within the part.

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS tab_mat_scoring;
DROP TABLE IF EXISTS tab_mat_scoring_ref;

-- Only a wide part in full storage materializes the index in temporary segments: a compact or a packed part is
-- rewritten as a whole and the writer then builds the index inline, so `min_bytes_for_full_part_storage` is pinned
-- (CI randomizes it). `index_granularity` is pinned because the mutation reads the part in granule-aligned blocks
-- and a segment is flushed only between blocks, so the materialization must see several small blocks.
CREATE TABLE tab_mat_scoring
(
    id UInt64,
    s String
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 1024,
         text_index_posting_list_codec = 'bitpacking', text_index_max_processed_tokens_before_flush = 10000,
         allow_experimental_text_index_scoring = 1;

-- 'common': every row; 'freq<n>': 200 rows each; 'mid<n>': 8 rows each; 'filler' varies the document lengths.
INSERT INTO tab_mat_scoring SELECT
    number AS id,
    concat('common freq', toString(id % 100), ' mid', toString(id % 2500), repeat(' filler', id % 7))
FROM numbers(20000);

-- BM25 statistics are per part: both tables must consist of a single part.
OPTIMIZE TABLE tab_mat_scoring FINAL;

ALTER TABLE tab_mat_scoring ADD INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, enable_scoring = 1);
ALTER TABLE tab_mat_scoring MATERIALIZE INDEX idx;

SYSTEM FLUSH LOGS part_log;

SELECT '-- the index was materialized from several temporary segments';
SELECT ProfileEvents['TextIndexTemporarySegmentsWritten'] > 1
FROM system.part_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND database = currentDatabase()
    AND table = 'tab_mat_scoring'
    AND event_type = 'MutatePart'
    AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- The reference: the same rows indexed with scoring at insert time, in one part.
CREATE TABLE tab_mat_scoring_ref
(
    id UInt64,
    s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, enable_scoring = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1024,
         text_index_posting_list_codec = 'bitpacking', allow_experimental_text_index_scoring = 1;

INSERT INTO tab_mat_scoring_ref SELECT id, s FROM tab_mat_scoring;
OPTIMIZE TABLE tab_mat_scoring_ref FINAL;

SELECT '-- filtering over the materialized index';
SELECT count() FROM tab_mat_scoring WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_mat_scoring WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_mat_scoring WHERE hasToken(s, 'mid123') SETTINGS use_skip_indexes = 1;

SELECT '-- scores over the materialized index match the index built at insert time';
SELECT count(), countIf(abs(mat.score - ref.score) > 1e-6)
FROM
(
    SELECT id, _bm25_score AS score FROM tab_mat_scoring
    WHERE hasAnyTokens(s, ['freq7', 'mid123', 'filler'])
) AS mat
INNER JOIN
(
    SELECT id, _bm25_score AS score FROM tab_mat_scoring_ref
    WHERE hasAnyTokens(s, ['freq7', 'mid123', 'filler'])
) AS ref ON mat.id = ref.id;

SELECT '-- top-k with dynamic filtering uses the block-max metadata of the materialized index';
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;

SELECT id, round(_bm25_score, 6) FROM tab_mat_scoring
WHERE hasToken(s, 'freq7') ORDER BY _bm25_score DESC, id LIMIT 5;

SELECT id, round(_bm25_score, 6) FROM tab_mat_scoring_ref
WHERE hasToken(s, 'freq7') ORDER BY _bm25_score DESC, id LIMIT 5;

DROP TABLE tab_mat_scoring;
DROP TABLE tab_mat_scoring_ref;
