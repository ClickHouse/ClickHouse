-- For tables with a text index, the merge may produce a corrupt .mrk file format

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
SETTINGS index_granularity_bytes = 0, -- non-adaptive granularity
         min_bytes_for_wide_part = 0; -- avoid warning about non-adaptive granularity being incompatible with compact part format

INSERT INTO tab
SELECT
    2,
    multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
FROM numbers(1024000);

OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE hasToken(str, 'aa'); -- this must not return an error

DROP TABLE tab;
