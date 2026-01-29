SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = splitByNonAlpha),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, message)
VALUES
    (1, 'abc+ def- foo!'),
    (2, 'abc+ def- bar?'),
    (3, 'abc+ baz- foo!'),
    (4, 'abc+ baz- bar?'),
    (5, 'abc+ zzz- foo!'),
    (6, 'abc+ zzz- bar?');

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['ab']);
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['foo']);

DROP TABLE tab;
