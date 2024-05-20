-- Tags: no-random-merge-tree-settings

SET optimize_move_to_prewhere = 1;
SET convert_query_to_cnf = 0;
SET optimize_read_in_order = 1;

SET allow_experimental_analyzer = 1; -- slightly different operator names than w/o

DROP TABLE IF EXISTS test_skip_idx;

CREATE TABLE test_skip_idx (
    id UInt32,
    INDEX name_idx_g2 id TYPE minmax GRANULARITY 2,
    INDEX name_idx_g1 id TYPE minmax GRANULARITY 1)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO test_skip_idx SELECT number FROM system.numbers LIMIT 5 OFFSET 1;

EXPLAIN indexes = 1 SELECT * FROM test_skip_idx WHERE id < 2;
EXPLAIN indexes = 1 SELECT * FROM test_skip_idx WHERE id < 3;

DROP TABLE test_skip_idx;
