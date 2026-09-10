-- A two-level in-order merge forwards the virtual rows of its members through the preliminary
-- merges, so they pass every transform between the preliminary and the final merge, here a
-- preliminary DISTINCT. A forwarded virtual row must stay an announcement (an empty chunk), or
-- the DISTINCT remembers its key and drops the real rows it announced: the first key of every
-- part in read order, i.e. the minimum of the table for ascending and the maximum for descending.

DROP TABLE IF EXISTS t_virtual_row_two_level_distinct;

CREATE TABLE t_virtual_row_two_level_distinct (a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

-- Keep the parts unmerged: the preliminary merges only exist for several parts.
SYSTEM STOP MERGES t_virtual_row_two_level_distinct;

INSERT INTO t_virtual_row_two_level_distinct SELECT number % 10, number % 7 FROM numbers(2000);
INSERT INTO t_virtual_row_two_level_distinct SELECT number % 10, number % 7 FROM numbers(2000, 2000);
INSERT INTO t_virtual_row_two_level_distinct SELECT number % 10, number % 7 FROM numbers(4000, 2000);
INSERT INTO t_virtual_row_two_level_distinct SELECT number % 10, number % 7 FROM numbers(6000, 2000);

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 1,
    max_threads = 2, max_block_size = 64;

-- DISTINCT per partition reads each partition through its own port and skips the merge
-- altogether, so there would be no preliminary merge to forward through.
SET allow_distinct_partitions_independently = 0, force_distinct_partitions_independently = 0;

-- The read must go through preliminary merges below the final one.
SELECT count() > 1
FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a, b)
WHERE explain LIKE '%MergingSortedTransform%';

-- Both DISTINCT implementations, both read directions: the result must match the plain read.
SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a, b SETTINGS optimize_distinct_in_order = 1))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a, b SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a DESC, b DESC SETTINGS optimize_distinct_in_order = 1))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a DESC, b DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a, b SETTINGS optimize_distinct_in_order = 0))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a, b SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

SELECT
    (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a DESC, b DESC SETTINGS optimize_distinct_in_order = 0))
  = (SELECT groupArray((a, b)) FROM (SELECT DISTINCT a, b FROM t_virtual_row_two_level_distinct ORDER BY a DESC, b DESC SETTINGS optimize_read_in_order = 0, read_in_order_use_virtual_row = 0));

DROP TABLE t_virtual_row_two_level_distinct;
