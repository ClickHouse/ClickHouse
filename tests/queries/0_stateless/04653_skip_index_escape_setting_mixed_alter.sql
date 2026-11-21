-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: a Replicated database refuses an ALTER that mixes these commands (QUERY_IS_PROHIBITED).
-- no-shared-merge-tree: SharedMergeTree has its own ALTER path.

-- Turning escaping on in the ALTER that adds the index makes the pair legal, so the published
-- metadata must use the escaped names too, or `a.dct` overwrites the text index's dictionary.
CREATE TABLE t_collide (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, packed_skip_index_max_bytes = 0,
         escape_index_filenames = 0;
ALTER TABLE t_collide ADD INDEX `a.dct` w TYPE set(100) GRANULARITY 1, MODIFY SETTING escape_index_filenames = 1;
INSERT INTO t_collide SELECT number, concat('hello', toString(number % 10)), number FROM numbers(500);
OPTIMIZE TABLE t_collide FINAL;
SELECT count() FROM t_collide;
DROP TABLE t_collide;

-- The ReplicatedMergeTree counterpart: a replicated command, a comment and the setting in one ALTER.
CREATE TABLE t_collide_r (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_collide_r', '1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, packed_skip_index_max_bytes = 0,
         escape_index_filenames = 0;
ALTER TABLE t_collide_r ADD INDEX `a.dct` w TYPE set(100) GRANULARITY 1, MODIFY COMMENT 'c',
    MODIFY SETTING escape_index_filenames = 1;
INSERT INTO t_collide_r SELECT number, concat('hello', toString(number % 10)), number FROM numbers(500);
SELECT count() FROM t_collide_r WHERE s LIKE '%hello3%' SETTINGS force_data_skipping_indices = 'a', use_skip_indexes = 1;
DROP TABLE t_collide_r;
