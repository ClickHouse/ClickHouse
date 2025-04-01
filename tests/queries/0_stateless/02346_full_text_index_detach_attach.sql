-- Test detaching/attaching parts of a table with a full_text index

SET allow_experimental_full_text_index = 1;

CREATE TABLE tab
(
    k UInt64,
    v String,
    INDEX idx v TYPE full_text(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k;

INSERT INTO tab VALUES (1, 'Hello World');

ALTER TABLE tab DETACH PART 'all_1_1_0';
ALTER TABLE tab ATTACH PART 'all_1_1_0';
