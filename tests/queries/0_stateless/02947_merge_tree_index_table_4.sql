-- Tags: no-random-settings

DROP TABLE IF EXISTS t_merge_tree_index;

SET output_format_pretty_row_numbers = 0;
SET print_pretty_type_names = 0;

CREATE TABLE t_merge_tree_index
(
    `a` UInt64,
    `b` UInt64,
    `sp` UInt64,
    `arr` Array(LowCardinality(String)),
    `n` Nested(c1 String, c2 UInt64),
    `t` Tuple(c1 UInt64, c2 UInt64),
    `column.with.dots` UInt64
)
ENGINE = MergeTree
ORDER BY (a, b, sipHash64(sp) % 100)
SETTINGS
    index_granularity = 3,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 6,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    serialization_info_version = 'basic',
    write_marks_for_substreams_in_compact_parts=1;

SYSTEM STOP MERGES t_merge_tree_index;

INSERT INTO t_merge_tree_index SELECT number % 5, number, 0, ['foo', 'bar'], ['aaa', 'bbb', 'ccc'], [11, 22, 33], (number, number), number FROM numbers(10);

ALTER TABLE t_merge_tree_index ADD COLUMN c UInt64 AFTER b;

INSERT INTO t_merge_tree_index SELECT number % 5, number, number, 10, ['foo', 'bar'], ['aaa', 'bbb', 'ccc'], [11, 22, 33], (number, number), number FROM numbers(5);
INSERT INTO t_merge_tree_index SELECT number % 5, number, number, 10, ['foo', 'bar'], ['aaa', 'bbb', 'ccc'], [11, 22, 33], (number, number), number FROM numbers(10);

SELECT * FROM mergeTreeIndex(currentDatabase(), t_merge_tree_index) ORDER BY part_name, mark_number FORMAT PrettyCompactNoEscapesMonoBlock;
SELECT * FROM mergeTreeIndex(currentDatabase(), t_merge_tree_index, with_marks = true) ORDER BY part_name, mark_number FORMAT PrettyCompactNoEscapesMonoBlock;

SET describe_compact_output = 1;
DESCRIBE mergeTreeIndex(currentDatabase(), t_merge_tree_index, with_marks = true);

DROP TABLE t_merge_tree_index;

