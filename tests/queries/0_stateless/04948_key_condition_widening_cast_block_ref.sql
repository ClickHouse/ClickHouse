-- A condition that wraps the primary key in a widening integer cast and then applies another
-- monotonic function on top of it must return exactly the rows a table without a sort key returns.
-- Every check prints (count, sum) for the sorted table next to (count, sum) for an identical table
-- with no sort key, so the two tuples on a line must be equal.

DROP TABLE IF EXISTS t_wpk_edge;
DROP TABLE IF EXISTS t_wpk_edge_unsorted;
DROP TABLE IF EXISTS t_wpk_chain;
DROP TABLE IF EXISTS t_wpk_chain_unsorted;
DROP TABLE IF EXISTS t_wpk_unsigned;
DROP TABLE IF EXISTS t_wpk_unsigned_unsorted;

CREATE TABLE t_wpk_edge (x Int16) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8;
CREATE TABLE t_wpk_edge_unsorted (x Int16) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_wpk_edge SELECT toInt64(number) - 32768 FROM numbers(16);
INSERT INTO t_wpk_edge_unsorted SELECT toInt64(number) - 32768 FROM numbers(16);

CREATE TABLE t_wpk_chain (x Int16) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8;
CREATE TABLE t_wpk_chain_unsorted (x Int16) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_wpk_chain SELECT toInt64(number) - 50 FROM numbers(64);
INSERT INTO t_wpk_chain_unsorted SELECT toInt64(number) - 50 FROM numbers(64);

CREATE TABLE t_wpk_unsigned (x UInt16) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8;
CREATE TABLE t_wpk_unsigned_unsorted (x UInt16) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_wpk_unsigned SELECT number FROM numbers(64);
INSERT INTO t_wpk_unsigned_unsorted SELECT number FROM numbers(64);

-- The cast reaches the smallest value of the key type, where the extra width changes the result.
SELECT 'open range',
    (SELECT (count(), sum(x)) FROM t_wpk_edge WHERE negate(toInt64(x)) >= 32760),
    (SELECT (count(), sum(x)) FROM t_wpk_edge_unsorted WHERE negate(toInt64(x)) >= 32760);

SELECT 'closed range',
    (SELECT (count(), sum(x)) FROM t_wpk_edge WHERE negate(toInt64(x)) BETWEEN 32760 AND 32768),
    (SELECT (count(), sum(x)) FROM t_wpk_edge_unsorted WHERE negate(toInt64(x)) BETWEEN 32760 AND 32768);

-- Two more functions after the cast.
SELECT 'two functions after cast',
    (SELECT (count(), sum(x)) FROM t_wpk_chain WHERE abs(negate(toInt64(x))) BETWEEN 10 AND 20),
    (SELECT (count(), sum(x)) FROM t_wpk_chain_unsorted WHERE abs(negate(toInt64(x))) BETWEEN 10 AND 20);

SELECT 'single point',
    (SELECT (count(), sum(x)) FROM t_wpk_chain WHERE abs(negate(toInt32(x))) = 50),
    (SELECT (count(), sum(x)) FROM t_wpk_chain_unsorted WHERE abs(negate(toInt32(x))) = 50);

SELECT 'functions in the other order',
    (SELECT (count(), sum(x)) FROM t_wpk_chain WHERE negate(abs(toInt64(x))) BETWEEN -20 AND -10),
    (SELECT (count(), sum(x)) FROM t_wpk_chain_unsorted WHERE negate(abs(toInt64(x))) BETWEEN -20 AND -10);

SELECT 'unsigned key',
    (SELECT (count(), sum(x)) FROM t_wpk_unsigned WHERE abs(negate(toUInt64(x))) BETWEEN 10 AND 20),
    (SELECT (count(), sum(x)) FROM t_wpk_unsigned_unsorted WHERE abs(negate(toUInt64(x))) BETWEEN 10 AND 20);

-- A cast alone still narrows the read down to the granules that can hold matching rows.
SELECT 'granules read', count() > 0
FROM (EXPLAIN indexes = 1 SELECT sum(x) FROM t_wpk_chain WHERE toInt64(x) BETWEEN -50 AND -40)
WHERE explain ILIKE '%Granules: 2/8%';

-- Without a cast the same shape was always correct, so a failure here means something else broke.
SELECT 'no cast',
    (SELECT (count(), sum(x)) FROM t_wpk_chain WHERE abs(negate(x)) BETWEEN 10 AND 20),
    (SELECT (count(), sum(x)) FROM t_wpk_chain_unsorted WHERE abs(negate(x)) BETWEEN 10 AND 20);

-- The chain shapes the fix is about must still prune. Result comparisons alone cannot show this: a
-- chain that declines analysis reads every granule and returns the same correct rows.
SET use_lightweight_primary_key_index_analysis = 1;
SELECT 'chain granules read, lightweight analysis', count() > 0
FROM (EXPLAIN indexes = 1 SELECT sum(x) FROM t_wpk_chain WHERE abs(negate(toInt64(x))) BETWEEN 10 AND 20)
WHERE explain ILIKE '%Granules: 5/8%';

SET use_lightweight_primary_key_index_analysis = 0;
SELECT 'chain granules read, full analysis', count() > 0
FROM (EXPLAIN indexes = 1 SELECT sum(x) FROM t_wpk_chain WHERE abs(negate(toInt64(x))) BETWEEN 10 AND 20)
WHERE explain ILIKE '%Granules: 5/8%';

DROP TABLE t_wpk_edge;
DROP TABLE t_wpk_edge_unsorted;
DROP TABLE t_wpk_chain;
DROP TABLE t_wpk_chain_unsorted;
DROP TABLE t_wpk_unsigned;
DROP TABLE t_wpk_unsigned_unsorted;
