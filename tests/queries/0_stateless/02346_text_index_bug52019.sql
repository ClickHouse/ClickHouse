-- Test for Bug 52019: Undefined behavior with text indexes

SET allow_experimental_full_text_index = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id UInt64,
    str Map(String, String),
    INDEX idx mapKeys(str) TYPE text(tokenizer = 'ngram', ngram_size = 2) GRANULARITY 1)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab (id) VALUES (0);

SELECT * FROM tab PREWHERE (str[NULL]) = 'Click a03';
SELECT * FROM tab PREWHERE (str[1]) = 'Click a03'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM tab PREWHERE (str['foo']) = 'Click a03';

DROP TABLE tab;
