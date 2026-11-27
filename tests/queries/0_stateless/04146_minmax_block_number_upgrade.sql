-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree: RMT/SMT allocate block numbers starting from 0

DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, p UInt64) ENGINE = MergeTree() PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'partition_key_only';

SYSTEM STOP MERGES t;

INSERT INTO t SELECT 1, 1; -- 1_1_1_0
INSERT INTO t SELECT 2, 1; -- 1_2_2_0
INSERT INTO t SELECT 3, 2; -- 2_3_3_0

-- query works before alter
select id from t where _block_number > 1 order by id;

-- enable virtuals columns in minmax
alter table t modify setting part_minmax_index_columns='with_block_number_offset' settings alter_sync=2;
INSERT INTO t SELECT 4, 2; -- 2_4_4_0

-- query works after alter
select id from t where _block_number > 1 order by id;

-- disable virtuals columns in minmax
alter table t modify setting part_minmax_index_columns='partition_key_only' settings alter_sync=2;
INSERT INTO t SELECT 5, 1; -- 1_5_5_0

-- query works after alter
select id from t where _block_number > 1 order by id;
