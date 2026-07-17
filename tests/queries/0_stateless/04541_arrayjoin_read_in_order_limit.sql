-- Regression test for the read-in-order limit pushdown with an `arrayJoin` function in the select
-- list. `arrayJoin` changes row cardinality (an empty array drops its base row), so a later row
-- must remain free to fill the `LIMIT`; the storage must not stop reading after the first base rows
-- because of the limit. This mirrors the `actions.hasArrayJoin()` fence in `buildSortingDAG`
-- (`optimizeReadInOrder.cpp`) for the query-plan path and the `selectListHasArrayJoinFunction`
-- guard in `maxBlockSizeByLimit`, and covers the legacy `InterpreterSelectQuery` read-in-order path.

DROP TABLE IF EXISTS t_aj_riorder;

CREATE TABLE t_aj_riorder (pk UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY pk SETTINGS index_granularity = 8;

-- The first 100 rows (by the sort key `pk`) have empty arrays; the first non-empty value is at pk = 100.
INSERT INTO t_aj_riorder SELECT number, if(number < 100, [], [number + 1000]) FROM numbers(300);

-- Must return the first three values in `pk` order (1100, 1101, 1102), never fewer, regardless of
-- whether the query-plan read-in-order optimization or the legacy interpreter path computes the limit.
SELECT arrayJoin(a) FROM t_aj_riorder ORDER BY pk LIMIT 3
    SETTINGS optimize_read_in_order = 1, enable_analyzer = 0, query_plan_read_in_order = 0, max_threads = 1, enable_parallel_replicas = 0;
SELECT '--';
SELECT arrayJoin(a) FROM t_aj_riorder ORDER BY pk LIMIT 3
    SETTINGS optimize_read_in_order = 1, enable_analyzer = 0, query_plan_read_in_order = 1, max_threads = 1, enable_parallel_replicas = 0;
SELECT '--';
SELECT arrayJoin(a) FROM t_aj_riorder ORDER BY pk LIMIT 3
    SETTINGS optimize_read_in_order = 1, enable_analyzer = 1, query_plan_read_in_order = 0, max_threads = 1, enable_parallel_replicas = 0;
SELECT '--';
SELECT arrayJoin(a) FROM t_aj_riorder ORDER BY pk LIMIT 3
    SETTINGS optimize_read_in_order = 1, enable_analyzer = 1, query_plan_read_in_order = 1, max_threads = 1, enable_parallel_replicas = 0;

DROP TABLE t_aj_riorder;
