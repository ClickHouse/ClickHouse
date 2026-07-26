CREATE TABLE t1 (key Int32, value DateTime) ENGINE = Log;
INSERT INTO  t1 select number, number from numbers(10000);
create table t2 ENGINE = Log as select key as key1, value from t1;

explain actions=1 select count() from
(SELECT key from t1 CROSS JOIN t2
  where t1.value >= toDateTime(toString(t2.value))
) where key = 162601
settings enable_analyzer=1, query_plan_remove_unused_columns=1, query_plan_merge_filters=1; -- CI may inject False for remove_unused_columns (unused __table2.key not pruned) or merge_filters (two WHERE conditions not merged+pushed, left as separate Filter steps)
