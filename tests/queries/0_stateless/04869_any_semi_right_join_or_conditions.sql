-- Tags: no-old-analyzer

-- ANY/SEMI RIGHT JOIN emits every row of its right table at most once, and exactly once when the
-- row has a match. With OR-ed conditions in ON a right row is reachable through several hash maps,
-- so the row must be claimed per row and not per key.
-- An ANY LEFT JOIN becomes an ANY RIGHT JOIN when the planner swaps the tables.

DROP TABLE IF EXISTS t_or_left;
DROP TABLE IF EXISTS t_or_head;
DROP TABLE IF EXISTS t_or_inner;
DROP TABLE IF EXISTS t_or_groups;
DROP TABLE IF EXISTS t_or_probes;

-- The sort keys pin the order the rows are stored and probed in, which the two focused cases below
-- rely on to reproduce the failure.
CREATE TABLE t_or_left (a1 Int32, b1 Int32) ENGINE = MergeTree ORDER BY a1;
CREATE TABLE t_or_head (a2 Int32, b2 Int32, probe_order UInt8) ENGINE = MergeTree ORDER BY probe_order;
CREATE TABLE t_or_inner (a2 Int32, b2 Int32, probe_order UInt8) ENGINE = MergeTree ORDER BY probe_order;

INSERT INTO t_or_left VALUES (1, 2), (2, 2), (3, 2), (4, 2);
-- (2, 3) matches all four left rows through the first condition, (7, 3) matches only the first
-- left row and only through the second one, and is probed first.
INSERT INTO t_or_head VALUES (7, 3, 0), (2, 3, 1);
-- The same, but the row matched only through the second condition is not the first left row.
INSERT INTO t_or_inner VALUES (6, 4, 0), (2, 3, 1);

SELECT 'one row per left row, no defaults';
SELECT a1, b2 FROM t_or_left ANY LEFT JOIN t_or_head ON b1 + 1 = a2 + 1 OR a1 + 4 = b2 + 2
ORDER BY a1 SETTINGS max_threads = 1, query_plan_join_swap_table = 'true';

SELECT 'count, distinct left rows, rows padded with defaults';
SELECT count(), uniqExact(a1), countIf(b2 = 0) FROM
(
    SELECT a1, b2 FROM t_or_left ANY LEFT JOIN t_or_inner ON b1 + 1 = a2 + 1 OR a1 + 4 = b2 + 2
) SETTINGS max_threads = 1, query_plan_join_swap_table = 'true';

-- 100 groups of 10 left rows sharing `b1`, so that one probe row matches a whole group at once,
-- while other probe rows match a single row of the group through the second condition.
CREATE TABLE t_or_groups (a1 Int32, b1 Int32) ENGINE = MergeTree ORDER BY (b1, a1);
CREATE TABLE t_or_probes (a2 Int32, b2 Int32) ENGINE = MergeTree ORDER BY a2;
INSERT INTO t_or_groups SELECT number + 1, intDiv(number, 10) + 1 FROM numbers(1000);
-- 50 more left rows that match no probe row at all: RIGHT ANY emits them once padded with
-- defaults through the non-joined stream, RIGHT SEMI does not emit them.
INSERT INTO t_or_groups SELECT number + 500000, 500000 FROM numbers(50);
INSERT INTO t_or_probes SELECT number + 1, 100000 FROM numbers(100);
INSERT INTO t_or_probes SELECT 200000, number * 10 + 1 FROM numbers(100);
INSERT INTO t_or_probes SELECT 200000, number * 10 + 6 FROM numbers(100);

SELECT 'swapped ANY LEFT JOIN';
SELECT count(), uniqExact(a1), countIf(a2 = 0) FROM
(
    SELECT a1, a2 FROM t_or_groups ANY LEFT JOIN t_or_probes ON b1 = a2 OR a1 = b2
) SETTINGS query_plan_join_swap_table = 'true';

SELECT 'ANY RIGHT JOIN';
SELECT count(), uniqExact(a1), countIf(a2 = 0) FROM
(
    SELECT a1, a2 FROM t_or_probes ANY RIGHT JOIN t_or_groups ON a2 = b1 OR b2 = a1
) SETTINGS query_plan_join_swap_table = 'false';

SELECT 'SEMI RIGHT JOIN';
SELECT count(), uniqExact(a1) FROM
(
    SELECT a1, a2 FROM t_or_probes SEMI RIGHT JOIN t_or_groups ON a2 = b1 OR b2 = a1
) SETTINGS query_plan_join_swap_table = 'false';

SELECT 'swapped ANY LEFT JOIN, joined blocks split';
SELECT count(), uniqExact(a1), countIf(a2 = 0) FROM
(
    SELECT a1, a2 FROM t_or_groups ANY LEFT JOIN t_or_probes ON b1 = a2 OR a1 = b2
) SETTINGS query_plan_join_swap_table = 'true', max_joined_block_size_rows = 7;

SELECT 'SEMI RIGHT JOIN, joined blocks split';
SELECT count(), uniqExact(a1) FROM
(
    SELECT a1, a2 FROM t_or_probes SEMI RIGHT JOIN t_or_groups ON a2 = b1 OR b2 = a1
) SETTINGS query_plan_join_swap_table = 'false', max_joined_block_size_rows = 7;

-- A condition over both tables that survives as a residual is executed by another code path,
-- which claims the rows after applying the residual filter.
SELECT 'swapped ANY LEFT JOIN, residual condition';
SELECT count(), uniqExact(a1), countIf(a2 = 0) FROM
(
    SELECT a1, a2 FROM t_or_groups ANY LEFT JOIN t_or_probes ON (b1 = a2 OR a1 = b2) AND a1 + a2 != -12345
) SETTINGS query_plan_join_swap_table = 'true';

SELECT 'SEMI RIGHT JOIN, residual condition';
SELECT count(), uniqExact(a1) FROM
(
    SELECT a1, a2 FROM t_or_probes SEMI RIGHT JOIN t_or_groups ON (a2 = b1 OR b2 = a1) AND a1 + a2 != -12345
) SETTINGS query_plan_join_swap_table = 'false';

DROP TABLE t_or_left;
DROP TABLE t_or_head;
DROP TABLE t_or_inner;
DROP TABLE t_or_groups;
DROP TABLE t_or_probes;
