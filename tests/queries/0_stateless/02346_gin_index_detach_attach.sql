-- Test that detaching and attaching parts with a GIN index works

SET allow_experimental_full_text_index = 1;

CREATE TABLE t
(
    key UInt64,
    str String,
    INDEX inv_idx str TYPE gin(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_full_part_storage = 0; -- GIN indexes currently don't work with packed parts

INSERT INTO t VALUES (1, 'Hello World');

ALTER TABLE t DETACH PART 'all_1_1_0';
ALTER TABLE t ATTACH PART 'all_1_1_0';
