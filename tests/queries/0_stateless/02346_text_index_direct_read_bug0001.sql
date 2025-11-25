-- 03580_text_index_direct_read_optimization_bug0001.sql

SET allow_experimental_full_text_index = 1;
SET allow_experimental_analyzer = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id   UInt64,
    text String,
    INDEX inv_idx text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab VALUES
    (1, 'alpha beta gamma'),
    (2, 'alpha gamma'),
    (3, 'beta gamma'),
    (4, 'alpha beta'),
    (5, 'alpha something beta'),
    (6, 'prefix alpha beta suffix'),
    (7, 'alpha'),
    (8, 'delta'),
    (9, 'beta alpha');

SELECT text like '%suffix%' FROM tab WHERE text LIKE '%suffix%';

SELECT text like '%suffix%' FROM tab WHERE id = 5;

SELECT text like '%suffix%' FROM tab WHERE text LIKE '%suffix%' or id = 8;
