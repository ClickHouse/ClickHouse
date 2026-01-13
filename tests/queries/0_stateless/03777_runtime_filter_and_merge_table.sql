SET enable_analyzer=1;

DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS foo1;
DROP TABLE IF EXISTS foo_merge;
DROP TABLE IF EXISTS t2;

CREATE TABLE foo(Id Int32, Val Int32) Engine=MergeTree PARTITION BY Val ORDER BY Id;
CREATE TABLE foo1(Id Int32, Val Decimal32(9)) Engine=MergeTree PARTITION BY Val ORDER BY Id;
INSERT INTO foo SELECT number, number%5 FROM numbers(100000);
INSERT INTO foo1 SELECT number, 1 FROM numbers(100000);

CREATE TABLE foo_merge as foo ENGINE=Merge(currentDatabase(), '^foo');

CREATE TABLE t2 (Id Int32, Val Int64, X UInt256) Engine=Memory;
INSERT INTO t2 values (4, 3, 4);

SELECT REGEXP_REPLACE(explain, '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
EXPLAIN --header=1, actions=1
SELECT count()
FROM foo_merge JOIN t2 USING Val
SETTINGS enable_join_runtime_filters=1, parallel_replicas_local_plan=1
);

SELECT '=================================================';

SELECT count()
FROM foo_merge JOIN t2 USING Val
SETTINGS enable_join_runtime_filters=0;

SELECT '=================================================';

SELECT count()
FROM foo_merge JOIN t2 USING Val
SETTINGS enable_join_runtime_filters=1;
