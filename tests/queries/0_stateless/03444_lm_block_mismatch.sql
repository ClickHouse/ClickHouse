DROP TABLE IF EXISTS test_03444_lazy;
CREATE TABLE test_03444_lazy (n UInt32) ENGINE = MergeTree ORDER BY n;
INSERT INTO test_03444_lazy SELECT * FROM generateRandom() LIMIT 50;

-- make sure the optimization is enabled
set query_plan_optimize_lazy_materialization=true, query_plan_max_limit_for_lazy_materialization=10;
SELECT count() FROM (SELECT * FROM test_03444_lazy ORDER BY rand() LIMIT 5);

select trimLeft(explain) as s from (EXPLAIN SELECT * FROM test_03444_lazy ORDER BY rand() LIMIT 5) where s ilike 'LazilyRead%';

DROP TABLE test_03444_lazy;
