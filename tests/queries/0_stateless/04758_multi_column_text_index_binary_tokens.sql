SET enable_full_text_index = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS multi_text_binary_tokens;

CREATE TABLE multi_text_binary_tokens
(
    id UInt64,
    left_text String,
    right_text String,
    INDEX idx (left_text, right_text) TYPE text(
        tokenizer = ngrams(3),
        field_ids = '{"left_text":255,"right_text":256}')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1,
    allow_experimental_multi_column_text_index = 1,
    max_bytes_to_merge_at_max_space_in_pool = 0;

-- Each insert stays in a separate part. The first two rows contain the same embedded-NUL token in
-- different fields; the third token shares its binary prefix.
INSERT INTO multi_text_binary_tokens VALUES (1, unhex('610062'), 'plain-left');
INSERT INTO multi_text_binary_tokens VALUES (2, 'plain-right', unhex('610062'));
INSERT INTO multi_text_binary_tokens VALUES (3, unhex('610063'), unhex('610063'));

SELECT 'left_nul', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_binary_tokens
    WHERE left_text = unhex('610062')
    ORDER BY id
);

SELECT 'right_nul', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_binary_tokens
    WHERE right_text = unhex('610062')
    ORDER BY id
);

SELECT 'index_pruning';
SELECT trimLeft(explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM multi_text_binary_tokens
    WHERE left_text = unhex('610062')
    SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes_on_data_read = 0
)
WHERE explain LIKE '%Name:%'
    OR explain LIKE '%Parts:%/%'
    OR explain LIKE '%Granules:%/%';

DROP TABLE multi_text_binary_tokens;
