-- For tables with a TextIndex, the index .mrk file format
-- generated during data merges has a compatibility issue.

SET allow_experimental_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    i Int32,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha'
    )
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity_bytes = 0;
   
INSERT INTO tab
SELECT
    2,
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE hasToken(str, 'aa');

DROP TABLE tab;
