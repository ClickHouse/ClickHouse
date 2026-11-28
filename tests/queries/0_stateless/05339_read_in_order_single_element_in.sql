-- A single-element `IN` on a sorting-key prefix fixes that column, like `=` does,
-- so `ORDER BY` on the next key column can read in order.

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_lazy_materialization = 0;

DROP TABLE IF EXISTS t_read_in_order_single_in;

CREATE TABLE t_read_in_order_single_in (owner String, project String, ts DateTime, value UInt32)
ENGINE = MergeTree ORDER BY (owner, project, ts);

INSERT INTO t_read_in_order_single_in SELECT 'o', toString(number % 3), toDateTime('2026-01-01 00:00:00') + number, number FROM numbers(30);

-- { echoOn }

SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM t_read_in_order_single_in WHERE owner = 'o' AND project = '1' ORDER BY ts DESC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM t_read_in_order_single_in WHERE owner = 'o' AND project IN ('1') ORDER BY ts DESC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM t_read_in_order_single_in WHERE owner = 'o' AND project IN ['1'] ORDER BY ts DESC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM t_read_in_order_single_in WHERE owner IN ('o') AND project IN ('1') ORDER BY ts ASC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM t_read_in_order_single_in WHERE owner = 'o' AND project IN ('1', '1') ORDER BY ts DESC LIMIT 3) WHERE explain LIKE '%ReadType%';

-- Several elements: the column is not fixed, so the read is not in order.
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM t_read_in_order_single_in WHERE owner = 'o' AND project IN ('1', '2') ORDER BY ts DESC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM t_read_in_order_single_in WHERE owner = 'o' AND project NOT IN ('1') ORDER BY ts DESC LIMIT 3) WHERE explain LIKE '%ReadType%';

SELECT project, ts FROM t_read_in_order_single_in WHERE owner = 'o' AND project IN ('1') ORDER BY ts DESC LIMIT 3;
SELECT project, ts FROM t_read_in_order_single_in WHERE owner IN ('o') AND project IN ['2'] ORDER BY ts ASC LIMIT 3;
SELECT project, ts FROM t_read_in_order_single_in WHERE owner = 'o' AND project IN ('1', '2') ORDER BY ts DESC LIMIT 3;

-- { echoOff }

DROP TABLE t_read_in_order_single_in;

-- A shipped subquery set keeps the subquery's type: `d IN (SELECT 1.0)` converts `d` to `Float64`,
-- which matches several `Decimal` values, so `d` is not fixed and the sort is required.
DROP TABLE IF EXISTS t_read_in_order_single_in_decimal;
CREATE TABLE t_read_in_order_single_in_decimal (d Decimal(38, 18), ts UInt32) ENGINE = MergeTree ORDER BY (d, ts);
INSERT INTO t_read_in_order_single_in_decimal VALUES ('1', 20), ('1', 30), ('1.000000000000000001', 10), ('1.000000000000000001', 20);

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_read_in_order = 1, distributed_plan_default_reader_bucket_count = 1, distributed_plan_default_shuffle_join_bucket_count = 1,
    distributed_plan_max_rows_to_broadcast = 0, rewrite_in_to_join = 0;

-- { echoOn }
SELECT ts FROM t_read_in_order_single_in_decimal WHERE d IN (SELECT 1.0) ORDER BY ts LIMIT 1;
SELECT ts FROM t_read_in_order_single_in_decimal WHERE d IN (SELECT 1.0) ORDER BY ts;
-- { echoOff }

DROP TABLE t_read_in_order_single_in_decimal;
