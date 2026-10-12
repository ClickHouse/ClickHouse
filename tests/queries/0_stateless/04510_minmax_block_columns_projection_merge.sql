SET send_logs_level = 'error';

CREATE TABLE t
(
    x UInt8,
    PROJECTION p (SELECT x GROUP BY x)
)
ENGINE = MergeTree
ORDER BY ()
SETTINGS part_minmax_index_columns = 'with_block_number_offset', enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t VALUES (0);
OPTIMIZE TABLE t FINAL;

SELECT x FROM t;

DROP TABLE t;
