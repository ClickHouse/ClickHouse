DROP TABLE IF EXISTS t;

SET allow_table_engine_log=1;

CREATE TABLE t ENGINE = Log AS SELECT * FROM system.numbers LIMIT 20;
SET enable_optimize_predicate_expression = 0;
SELECT number FROM (select number FROM t ORDER BY number OFFSET 3) WHERE number < NULL;

DROP table t;
