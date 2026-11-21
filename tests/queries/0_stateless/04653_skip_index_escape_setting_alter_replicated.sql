-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: SharedMergeTree has its own ALTER path.

-- The ReplicatedMergeTree counterpart of the `auto_statistics_types` case in 04653. Turning escaping
-- on makes `a.dct` legal next to text index `a`, so the republished metadata must use the escaped
-- names too, or the set index overwrites the text index's dictionary and the text index drops every
-- granule. The mixed-ALTER counterpart lives in 04653_skip_index_escape_setting_mixed_alter.
CREATE TABLE t_collide_r (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_collide_r', '1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, packed_skip_index_max_bytes = 0,
         escape_index_filenames = 0, auto_statistics_types = 'minmax';
ALTER TABLE t_collide_r MODIFY SETTING escape_index_filenames = 1;
ALTER TABLE t_collide_r ADD INDEX `a.dct` w TYPE set(100) GRANULARITY 1;
INSERT INTO t_collide_r SELECT number, concat('hello', toString(number % 10)), number FROM numbers(500);
SELECT count() FROM t_collide_r WHERE s LIKE '%hello3%' SETTINGS force_data_skipping_indices = 'a', use_skip_indexes = 1;
DROP TABLE t_collide_r;
