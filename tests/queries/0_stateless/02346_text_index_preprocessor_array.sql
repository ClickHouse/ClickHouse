-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache and global udf factory

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

-- Tests the preprocessor argument for tokenizers in the text index definitions

DROP TABLE IF EXISTS tab;

SELECT 'Positive tests on preprocessor construction and use with array columns.';

SELECT '- Test single tokenizer and preprocessor (lower) argument.';

CREATE TABLE tab
(
    id UInt32,
    comment String,
    arr Array(String),
    arr_fixed Array(FixedString(3)),
    INDEX idx(arr) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arr)),
    INDEX idx_fixed(arr_fixed) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arr_fixed)),
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES (0, 'lower', ['foo', 'bar'], ['foo', 'bar']);
INSERT INTO tab VALUES (1, 'upper', ['FOO', 'BAR'], ['FOO', 'BAR']);
INSERT INTO tab VALUES (2, 'capital', ['Foo', 'Bar'], ['Foo', 'Bar']);
INSERT INTO tab VALUES (3, 'empty', ['', ''], ['', '']);
INSERT INTO tab VALUES (4, 'dummy', ['baz', 'DEF'], ['Baz', 'Def']);

SELECT '-- with Lower String';
SELECT id, comment FROM tab WHERE hasAllToken(arr, 'foo') ORDER BY id;
SELECT id, comment FROM tab WHERE hasAnyToken(arr, 'bar') ORDER BY id;

SELECT id, comment FROM tab WHERE hasAllToken(arr_fixed, 'foo') ORDER BY id;
SELECT id, comment FROM tab WHERE hasAnyToken(arr_fixed, 'bar') ORDER BY id;

SELECT '-- with Upper String';
SELECT id, comment FROM tab WHERE hasAllToken(arr, 'FOO') ORDER BY id;
SELECT id, comment FROM tab WHERE hasAnyToken(arr, 'BAR') ORDER BY id;

SELECT id, comment FROM tab WHERE hasAllToken(arr_fixed, 'FOO') ORDER BY id;
SELECT id, comment FROM tab WHERE hasAnyToken(arr_fixed, 'BAR') ORDER BY id;


SELECT '-- with Empty Match';
SELECT id, comment FROM tab WHERE hasAnyToken(arr_fixed, '') ORDER BY id;

DROP TABLE tab;

