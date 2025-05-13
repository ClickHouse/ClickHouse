-- Test for Bug 52019: Undefined behavior with GIN indexes

SET allow_experimental_full_text_index = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id UInt64,
    str Map(String, String),
    INDEX idx mapKeys(str) TYPE gin(2) GRANULARITY 1)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi',
         min_bytes_for_full_part_storage = 0; -- GIN indexes currently don't work with packed parts

INSERT INTO tab (id) VALUES (0);

SELECT * FROM tab PREWHERE (str[NULL]) = 'Click a03';
SELECT * FROM tab PREWHERE (str[1]) = 'Click a03'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM tab PREWHERE (str['foo']) = 'Click a03';

DROP TABLE tab;
