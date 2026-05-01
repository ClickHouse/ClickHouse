-- Tests https://github.com/ClickHouse/ClickHouse/issues/103812

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    text String,
    INDEX idx(text) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, concat('hello', number % 100, ' world', number % 100) FROM numbers(1000);

-- All combinations of direct_read and add_hint settings should return the same result.
SELECT count() FROM tab WHERE hasPhrase(text, 'ello50 wor')
SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1;

SELECT count() FROM tab WHERE hasPhrase(text, 'ello50 wor')
SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 0;

SELECT count() FROM tab WHERE hasPhrase(text, 'ello50 wor')
SETTINGS query_plan_direct_read_from_text_index = 0, query_plan_text_index_add_hint = 1;

SELECT count() FROM tab WHERE hasPhrase(text, 'ello50 wor')
SETTINGS query_plan_direct_read_from_text_index = 0, query_plan_text_index_add_hint = 0;

DROP TABLE tab;
