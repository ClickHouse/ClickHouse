SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS t_like_bug;

CREATE TABLE t_like_bug
(
    id UInt32,
    text String,
    PROJECTION idx_text INDEX text TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0;

INSERT INTO t_like_bug SELECT number, 'common ' || toString(number) FROM numbers(100);

SELECT count() FROM t_like_bug WHERE hasToken(text, 'common');
SELECT count() FROM t_like_bug WHERE text LIKE '%common%';
SELECT count() FROM t_like_bug WHERE text ILIKE '%COMMON%';

DROP TABLE t_like_bug;
