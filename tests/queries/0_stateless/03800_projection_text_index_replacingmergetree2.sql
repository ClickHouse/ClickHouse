SET allow_experimental_projection_text_index = 1;
DROP TABLE IF EXISTS tab;

-- Tests text index with the 'ReplacingMergeTree' engine

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

CREATE TABLE tab
(
    id UInt64,
    version UInt64,
    text String,
    PROJECTION idx_text INDEX text TYPE text(tokenizer = array)
)
-- A randomized tiny `merge_max_block_size` makes the projection rebuild during the merge
-- below too slow for debug builds under the flaky check, so it is pinned.
ENGINE = ReplacingMergeTree(version) ORDER BY id SETTINGS deduplicate_merge_projection_mode = 'rebuild', merge_max_block_size = 8192;

INSERT INTO tab SELECT number, 1, 'v' || toString(number) FROM numbers(100000);
INSERT INTO tab SELECT number, 2, 'v' || toString(number) || '_updated' FROM numbers(0, 100000, 3);

OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE text = 'v12345';
SELECT count() FROM tab WHERE text = 'v12345_updated';

SELECT count() FROM tab FINAL WHERE text = 'v54320';
SELECT count() FROM tab WHERE text = 'v54320_updated';

DROP TABLE tab;
