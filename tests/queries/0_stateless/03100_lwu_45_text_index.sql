SET allow_experimental_full_text_index = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_lwu_text_index;

CREATE TABLE t_lwu_text_index
(
    str String,
    val UInt64,
    INDEX idx_str (str) TYPE text(tokenizer = ngrams(3)) GRANULARITY 8
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_text_index SELECT toString(number), number * 2 FROM numbers(1000000);

SELECT count() FROM t_lwu_text_index WHERE str LIKE '%777%' OR str LIKE '%888%';
SELECT count() FROM t_lwu_text_index WHERE hasAnyTokens(str, ['777', '888']);

SELECT sum(val) FROM t_lwu_text_index WHERE str LIKE '%777%' OR str LIKE '%888%';
SELECT sum(val) FROM t_lwu_text_index WHERE hasAnyTokens(str, ['777', '888']);

UPDATE t_lwu_text_index SET val = val + 1 WHERE str LIKE '%777%';

SELECT count() FROM t_lwu_text_index WHERE str LIKE '%777%' OR str LIKE '%888%';
SELECT count() FROM t_lwu_text_index WHERE hasAnyTokens(str, ['777', '888']);

SELECT sum(val) FROM t_lwu_text_index WHERE str LIKE '%777%' OR str LIKE '%888%';
SELECT sum(val) FROM t_lwu_text_index WHERE hasAnyTokens(str, ['777', '888']);

UPDATE t_lwu_text_index SET str = '' WHERE str LIKE '%888%';

SELECT count() FROM t_lwu_text_index WHERE str LIKE '%777%' OR str LIKE '%888%';
SELECT count() FROM t_lwu_text_index WHERE hasAnyTokens(str, ['777', '888']);

SELECT sum(val) FROM t_lwu_text_index WHERE str LIKE '%777%' OR str LIKE '%888%';
SELECT sum(val) FROM t_lwu_text_index WHERE hasAnyTokens(str, ['777', '888']);

DROP TABLE t_lwu_text_index;
