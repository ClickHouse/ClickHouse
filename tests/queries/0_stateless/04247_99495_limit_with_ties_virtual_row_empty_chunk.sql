-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS t_99495;

CREATE TABLE t_99495
(
    id UInt64,
    m Map(String, String),
    INDEX idx_mk mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_99495 VALUES (1, {'1': '1'});
INSERT INTO t_99495 VALUES (2, {'2': '2'});
INSERT INTO t_99495 VALUES (3, {'3': '3'});

SELECT id
FROM t_99495
PREWHERE mapContains(m, toFixedString('1', 1))
ORDER BY id ASC
LIMIT 1 WITH TIES
SETTINGS
    optimize_read_in_order = 1,
    read_in_order_use_virtual_row = 1,
    read_in_order_use_virtual_row_per_block = 1,
    use_skip_indexes_on_data_read = 0;

SELECT id
FROM t_99495
PREWHERE mapContains(m, toFixedString('1', 1))
ORDER BY id ASC
LIMIT 0, 1 WITH TIES
SETTINGS
    optimize_read_in_order = 1,
    read_in_order_use_virtual_row = 1,
    read_in_order_use_virtual_row_per_block = 1,
    use_skip_indexes_on_data_read = 0;

DROP TABLE t_99495;
