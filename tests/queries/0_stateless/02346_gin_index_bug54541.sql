-- Test for AST Fuzzer crash #54541

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX idx str TYPE gin
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_full_part_storage = 0; -- GIN indexes currently don't work with packed parts

INSERT INTO tab VALUES (0, 'a');
SELECT * FROM tab WHERE str == 'b' AND 1.0;

DROP TABLE IF EXISTS tab;
