-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `ORDER BY ... WITH FILL` can be executed under `make_distributed_plan = 1`: `FillingStep` is serialized
-- for remote execution, and the sort description carries the `FROM`/`TO`/`STEP`/`STALENESS` bounds. Before
-- that, `canExecuteRemotely` rejected the plan because `FillingStep` was not serializable, and the query
-- failed with "make_distributed_plan cannot distribute this query".
-- The fill runs above the sorted gather, so it must produce exactly the non-distributed result.

DROP TABLE IF EXISTS t_fill_dist;

CREATE TABLE t_fill_dist (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_fill_dist SELECT number * 4, toString(number) FROM numbers(200);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
    enable_join_runtime_filters = 0;
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, and the functional-test
-- profile (`tests/config/users.d/limits.yaml`) sets it to 10G, so pin it off.
SET max_rows_to_group_by = 0;

SELECT '-- STEP 2, whole stream';
SELECT count(), sum(a) FROM (SELECT a FROM t_fill_dist ORDER BY a WITH FILL STEP 2)
SETTINGS make_distributed_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_fill_dist ORDER BY a WITH FILL STEP 2);

SELECT '-- STEP 2, head';
SELECT a FROM t_fill_dist ORDER BY a WITH FILL STEP 2 LIMIT 6 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_fill_dist ORDER BY a WITH FILL STEP 2 LIMIT 6;

SELECT '-- FROM 2 TO 30 STEP 3';
SELECT a FROM t_fill_dist WHERE a < 20 ORDER BY a WITH FILL FROM 2 TO 30 STEP 3
SETTINGS make_distributed_plan = 0;
SELECT a FROM t_fill_dist WHERE a < 20 ORDER BY a WITH FILL FROM 2 TO 30 STEP 3;

SELECT '-- INTERPOLATE';
SELECT a, b FROM t_fill_dist ORDER BY a WITH FILL STEP 2 INTERPOLATE (b AS b) LIMIT 6
SETTINGS make_distributed_plan = 0;
SELECT a, b FROM t_fill_dist ORDER BY a WITH FILL STEP 2 INTERPOLATE (b AS b) LIMIT 6;

SELECT '-- DESC WITH FILL STALENESS';
SELECT a FROM t_fill_dist WHERE a > 780 ORDER BY a DESC WITH FILL STEP -2 STALENESS -6
SETTINGS make_distributed_plan = 0;
SELECT a FROM t_fill_dist WHERE a > 780 ORDER BY a DESC WITH FILL STEP -2 STALENESS -6;

-- An `INTERVAL` step keeps its kind (`step_kind`) next to the value, so a date/time fill needs both.
SELECT '-- DateTime WITH FILL STEP INTERVAL';
SELECT toDateTime('2020-01-01 00:00:00') + INTERVAL a SECOND AS ts
FROM t_fill_dist WHERE a < 12
ORDER BY ts WITH FILL STEP INTERVAL 1 SECOND
SETTINGS make_distributed_plan = 0;
SELECT toDateTime('2020-01-01 00:00:00') + INTERVAL a SECOND AS ts
FROM t_fill_dist WHERE a < 12
ORDER BY ts WITH FILL STEP INTERVAL 1 SECOND;

-- `FROM`/`TO` carry their own types (`fill_from_type`, `fill_to_type`), and a sub-second `INTERVAL`
-- carries its kind, so a `DateTime64` fill exercises every part of the serialized fill description.
SELECT '-- DateTime64 WITH FILL FROM/TO STEP INTERVAL 500 MILLISECOND';
SELECT count() FROM (
    SELECT toDateTime64('2020-01-01 00:00:00.000', 3) + INTERVAL a SECOND AS ts
    FROM t_fill_dist WHERE a < 12
    ORDER BY ts WITH FILL
        FROM toDateTime64('2019-12-31 23:59:58.000', 3)
        TO toDateTime64('2020-01-01 00:00:12.000', 3)
        STEP INTERVAL 500 MILLISECOND)
SETTINGS make_distributed_plan = 0;
SELECT count() FROM (
    SELECT toDateTime64('2020-01-01 00:00:00.000', 3) + INTERVAL a SECOND AS ts
    FROM t_fill_dist WHERE a < 12
    ORDER BY ts WITH FILL
        FROM toDateTime64('2019-12-31 23:59:58.000', 3)
        TO toDateTime64('2020-01-01 00:00:12.000', 3)
        STEP INTERVAL 500 MILLISECOND);

DROP TABLE t_fill_dist;
