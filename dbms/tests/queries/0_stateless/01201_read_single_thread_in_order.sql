DROP TABLE IF EXISTS t;

CREATE TABLE t
(
  number UInt64
)
ENGINE = MergeTree
ORDER BY number
SETTINGS index_granularity = 128;

INSERT INTO t SELECT number FROM numbers(10000000);

SET max_threads = 1, max_block_size = 12345;
SELECT arrayDistinct(arrayPopFront(arrayDifference(groupArray(number)))) FROM t;

DROP TABLE t;
