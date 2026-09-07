-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- Tracked in https://github.com/ClickHouse/ClickHouse/issues/118534

-- `rows_before_limit_at_least` is part of the JSON output whenever the query has a LIMIT, and
-- `exact_rows_before_limit = 1` makes it exact. In a distributed plan the final `Limit` runs inside a
-- worker task, and `QueryPipeline::initRowsBeforeLimit` on the initiator does not recognise
-- `ReadFromDistributedPlanSource` (it does handle `RemoteSource`), so the field is dropped. The output
-- must match `make_distributed_plan = 0`.

DROP TABLE IF EXISTS t_dp_rbl;
CREATE TABLE t_dp_rbl (k UInt32, v UInt64, s String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_dp_rbl SELECT number % 20, number, concat('s', toString(number % 7)) FROM numbers(10000);

SET enable_analyzer = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0, output_format_write_statistics = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;

SELECT s FROM t_dp_rbl WHERE v > 0 ORDER BY s LIMIT 3 SETTINGS exact_rows_before_limit = 1 FORMAT JSONCompact;
SELECT s FROM t_dp_rbl WHERE v > 0 ORDER BY s LIMIT 3 SETTINGS exact_rows_before_limit = 0 FORMAT JSONCompact;

DROP TABLE t_dp_rbl;
