SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1, max_block_size = 2;

DROP TABLE IF EXISTS t;

CREATE TABLE t (CounterID UInt32) ENGINE = MergeTree ORDER BY CounterID
SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t;

INSERT INTO t SELECT number * 2 + 1 FROM numbers(5);
INSERT INTO t SELECT number * 2 + 2 FROM numbers(5);

SELECT CounterID FROM t ORDER BY CounterID DESC;
SELECT '-';
SELECT CounterID FROM t ORDER BY CounterID;

DROP TABLE IF EXISTS t;
