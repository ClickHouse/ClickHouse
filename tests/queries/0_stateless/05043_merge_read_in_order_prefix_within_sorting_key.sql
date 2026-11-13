-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t_read_in_order_prefix;
DROP TABLE IF EXISTS m_read_in_order_prefix;

CREATE TABLE t_read_in_order_prefix (a UInt64, b UInt64, c UInt64)
    ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_read_in_order_prefix;

-- Two parts, so the preliminary merge path is taken with the threshold below.
INSERT INTO t_read_in_order_prefix SELECT number, number, number FROM numbers(500);
INSERT INTO t_read_in_order_prefix SELECT number, number, number FROM numbers(500);

CREATE TABLE m_read_in_order_prefix (a UInt64, b UInt64, c UInt64)
    ENGINE = Merge(currentDatabase(), '^t_read_in_order_prefix$');

-- Read-in-order is still chosen for a well-formed query over a Merge table, and the
-- preliminary merge path that consumes the key prefix is the one exercised.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT a, b, c FROM m_read_in_order_prefix ORDER BY a, b, c
    SETTINGS optimize_read_in_order = 1, read_in_order_two_level_merge_threshold = 1,
             explain_query_plan_default = 'pretty'
) WHERE explain ILIKE '%Read type: InOrder%';

-- Control: the assertion above is not vacuous.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT a, b, c FROM m_read_in_order_prefix ORDER BY a, b, c
    SETTINGS optimize_read_in_order = 0, explain_query_plan_default = 'pretty'
) WHERE explain ILIKE '%Read type: InOrder%';

-- Results are unchanged with and without the optimization.
SELECT a, b, c FROM m_read_in_order_prefix ORDER BY a, b, c LIMIT 3
    SETTINGS optimize_read_in_order = 1, read_in_order_two_level_merge_threshold = 1;
SELECT a, b, c FROM m_read_in_order_prefix ORDER BY a, b, c LIMIT 3
    SETTINGS optimize_read_in_order = 0;

DROP TABLE m_read_in_order_prefix;
DROP TABLE t_read_in_order_prefix;
