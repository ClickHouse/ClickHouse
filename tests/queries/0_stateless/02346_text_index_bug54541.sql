-- Test for AST Fuzzer crash #54541

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX idx str TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (0, 'a');
SELECT * FROM tab WHERE str == 'b' AND 1.0;

DROP TABLE IF EXISTS tab;
