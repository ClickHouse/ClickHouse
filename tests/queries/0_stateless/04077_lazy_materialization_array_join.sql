#!/usr/bin/env -S ${HOME}/clickhouse client --queries-file

SET query_plan_optimize_lazy_materialization = 1;
SET flatten_nested = 0;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0
(
    id UInt32,
    col1 Nested(a UInt32, n Nested(s String, b UInt32))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t0 SELECT number, arrayMap(x -> (x, arrayMap(y -> (toString(y * x), y + x), range(number % 17))), range(number % 19)) FROM numbers(10);

SELECT arrayJoin(col1.n) FROM t0 ORDER BY id LIMIT 2 OFFSET 2;

SELECT '----------------------';

SELECT arrayJoin(col1.n) FROM t0 ORDER BY id LIMIT 2 OFFSET 2 SETTINGS optimize_read_in_order=0, query_plan_execute_functions_after_sorting=0;
