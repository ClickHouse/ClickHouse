DROP TABLE IF EXISTS t_const_prewhere;

CREATE TABLE t_const_prewhere (id Int16, row_ver UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity_bytes = 0, index_granularity = 42, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_const_prewhere FORMAT VALUES (1, 1);

SELECT * FROM t_const_prewhere PREWHERE 1;
SELECT * FROM t_const_prewhere PREWHERE materialize(1);

DROP TABLE IF EXISTS t_const_prewhere;

CREATE TABLE t_const_prewhere (id Int16, row_ver UInt64)
ENGINE = MergeTree() ORDER BY id
SETTINGS index_granularity_bytes = '10M', index_granularity = 42;

INSERT INTO t_const_prewhere FORMAT VALUES (1, 1);

SELECT * FROM t_const_prewhere PREWHERE 1;
SELECT * FROM t_const_prewhere PREWHERE materialize(1);

DROP TABLE IF EXISTS t_const_prewhere;
