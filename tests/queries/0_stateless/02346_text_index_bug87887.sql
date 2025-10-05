-- Test for Bug 87887

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (c0 LowCardinality(String), INDEX i0 c0 TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab(c0) VALUES ('Hello, world!');
SELECT count() FROM tab WHERE hasToken(c0, 'Hello');
SELECT count() FROM tab WHERE hasToken(c0, 'Dummy');


DROP TABLE tab;
