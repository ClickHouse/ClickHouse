-- Test for Bugs 87887 and 88119

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (c0 LowCardinality(String), INDEX i0 c0 TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab(c0) VALUES ('Hello, world!');

SELECT 'Test Bug 87887';
SELECT 'Test hasToken(text, text):', count() FROM tab WHERE hasToken(c0, 'Hello');
SELECT 'Test hasToken(text, dummy):', count() FROM tab WHERE hasToken(c0, 'Dummy');

SELECT 'Test Bug 88119';
SELECT 'Test hasToken(text, nullable(text)):', count() FROM tab WHERE hasToken(c0, toNullable('Hello'));
SELECT 'Test hasToken(text, nullable(dummy)):', count() FROM tab WHERE hasToken(c0, toNullable('Dummy'));
SELECT 'Test hasToken(text, NULL):', count() FROM tab WHERE hasToken(c0, NULL);

DROP TABLE tab;
