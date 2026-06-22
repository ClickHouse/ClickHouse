-- A hasToken needle containing a token separator (whitespace or punctuation) is invalid and raises
-- BAD_ARGUMENTS at row level. A text index must not silently replace the predicate via Exact direct read
-- and hide that exception. hasTokenOrNull returns NULL per row instead of throwing.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/102497

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar baz'), (3, 'qux');

-- Mixed word/separator needles tokenize to several tokens; the index must not hide the row-level exception.
SELECT count() FROM tab WHERE hasToken(text, 'hello world'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE hasToken(text, 'foo.bar');     -- { serverError BAD_ARGUMENTS }

-- hasTokenOrNull returns NULL per row rather than throwing.
SELECT count() FROM tab WHERE isNull(hasTokenOrNull(text, 'hello world')); -- 3

-- Valid single-token needles still use the index and return the right counts.
SELECT count() FROM tab WHERE hasToken(text, 'foo'); -- 1
SELECT count() FROM tab WHERE hasToken(text, 'qux'); -- 1

DROP TABLE tab;
