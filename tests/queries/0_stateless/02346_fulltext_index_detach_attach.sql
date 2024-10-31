SET allow_experimental_full_text_index = 1;

CREATE TABLE t
(
    key UInt64,
    str String,
    INDEX inv_idx str TYPE full_text(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO t VALUES (1, 'Hello World');

ALTER TABLE t DETACH PART 'all_1_1_0';

ALTER TABLE t ATTACH PART 'all_1_1_0';
