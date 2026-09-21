-- Tags: long
-- ^ Reliably reproducing the boundary violation needs several unmerged parts with many small
-- granules, so a single run can exceed the 180s flaky-check limit under sanitizer builds.

-- Regression test for the "Virtual row boundary violated in MergingSortedAlgorithm" logical error
-- (STID 2651-3359). ORDER BY builds a read-in-order virtual row for the sort-key prefix it needs
-- (here CounterID). distinct-in-order then widens the read to a longer prefix (CounterID, EventDate)
-- without rebuilding the virtual row, so the extra column was default-filled and, in reverse order,
-- announced a boundary (0) smaller than the real values, tripping the assertion in debug builds and
-- silently mis-ordering the merge in release builds.

DROP TABLE IF EXISTS t_virtual_row_distinct;

CREATE TABLE t_virtual_row_distinct (CounterID UInt32, EventDate UInt64, s String)
ENGINE = MergeTree ORDER BY (CounterID, EventDate)
SETTINGS index_granularity = 8;

-- Keep the parts unmerged so the cross-part in-order merge (which consumes the virtual row) is
-- exercised deterministically instead of being collapsed into a single part by a background merge.
SYSTEM STOP MERGES t_virtual_row_distinct;

-- Several unmerged parts with small granules so an in-order merge across parts is used.
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(20000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(20000, 20000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(40000, 20000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(60000, 20000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(80000, 20000);
INSERT INTO t_virtual_row_distinct SELECT number % 5, 16000 + (number % 1000), toString(number) FROM numbers(100000, 20000);

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, optimize_distinct_in_order = 1,
    read_in_order_two_level_merge_threshold = 3, max_threads = 2, max_block_size = 64;

-- Must not throw and must return the correct distinct set.
-- Reverse order is the case that previously tripped the boundary check.
SELECT count() FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC);
SELECT count() FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY 1 ASC, CounterID DESC);

-- Result must match the unoptimized read.
SELECT
    (SELECT groupArray((CounterID, EventDate)) FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC, EventDate))
  = (SELECT groupArray((CounterID, EventDate)) FROM (SELECT DISTINCT CounterID, EventDate FROM t_virtual_row_distinct ORDER BY CounterID DESC, EventDate SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_distinct;

-- Fixed-key widening on key (a, b): ORDER BY a builds a one-column virtual row for a, then
-- distinct-in-order widens the read prefix to (a, b). The virtual row still covers only a, so b was
-- default-filled and reverse order announced b = 0 (its max), tripping the boundary check across
-- parts. Dropping the stale conversion on the widened re-request must fix this while the read stays
-- correct. This case fails without the requestReadingInOrder fix even though the fixed-middle-key
-- build-time guard below does not apply here (b is the last key, not a skipped middle key).
DROP TABLE IF EXISTS t_virtual_row_widen;

CREATE TABLE t_virtual_row_widen (a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_virtual_row_widen;

INSERT INTO t_virtual_row_widen SELECT number % 10, 1 FROM numbers(2000);
INSERT INTO t_virtual_row_widen SELECT number % 10, 1 FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_widen SELECT number % 10, 1 FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_widen SELECT number % 10, 1 FROM numbers(6000, 2000);

-- Must not throw and must match the unoptimized read.
SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen WHERE b = 1 ORDER BY a DESC SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, optimize_distinct_in_order = 1, read_in_order_two_level_merge_threshold = 3, max_threads = 2, max_block_size = 64))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_widen WHERE b = 1 ORDER BY a DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_widen;

-- A fixed MIDDLE key (WHERE b = 1 ORDER BY a, c on key (a, b, c)) is skipped without adding it to
-- the virtual row, so the columns after it are no longer a contiguous key prefix. The virtual row
-- builder indexes key columns densely, so the skipped key shifts every later column onto the wrong
-- key column: wrong value trips "Virtual row boundary violated" (STID 2651-3359), and a wrong type
-- (e.g. Nullable key) trips "Virtual row has different type" (STID 1637-309b). Virtual rows must be
-- disabled whenever a key column is skipped, and the reads must not throw and stay correct.
DROP TABLE IF EXISTS t_virtual_row_fixed_key;

CREATE TABLE t_virtual_row_fixed_key (a UInt32, b UInt32, c UInt32)
ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 8;

INSERT INTO t_virtual_row_fixed_key SELECT number % 10, 1, number % 7 FROM numbers(2000);
INSERT INTO t_virtual_row_fixed_key SELECT number % 10, 1, number % 7 FROM numbers(2000, 2000);

-- Virtual row optimization must be disabled for the skipped-middle-key read.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, c
      SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1)
WHERE explain ILIKE '%Virtual row conversions%';

-- Must not throw and must match the unoptimized read (previously STID 2651-3359).
SELECT
    (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, c SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1))
  = (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key WHERE b = 1 ORDER BY a, c SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_fixed_key;

-- Same skipped-middle-key path but with a Nullable key column, which previously threw the type
-- mismatch (STID 1637-309b) instead of the boundary violation.
DROP TABLE IF EXISTS t_virtual_row_fixed_key_nullable;

CREATE TABLE t_virtual_row_fixed_key_nullable (a UInt32, b UInt32, c Nullable(UInt32))
ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 8, allow_nullable_key = 1;

INSERT INTO t_virtual_row_fixed_key_nullable SELECT number % 10, 1, number % 7 FROM numbers(2000);
INSERT INTO t_virtual_row_fixed_key_nullable SELECT number % 10, 1, number % 7 FROM numbers(2000, 2000);

SELECT
    (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key_nullable WHERE b = 1 ORDER BY a ASC NULLS FIRST, c ASC SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1))
  = (SELECT groupArray((a, c)) FROM (SELECT a, c FROM t_virtual_row_fixed_key_nullable WHERE b = 1 ORDER BY a ASC NULLS FIRST, c ASC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_fixed_key_nullable;
