SET enable_analyzer = 1;

CREATE TABLE t1__fuzz_0 (`x` Nullable(UInt32), `str` String) ENGINE = Memory;
CREATE TABLE right_join__fuzz_0 (`x` UInt32, `s` String) ENGINE = Join(`ALL`, RIGHT, x);

EXPLAIN actions = 1, header = 1
SELECT
  *
FROM
  t1__fuzz_0 RIGHT JOIN right_join__fuzz_0 USING (x)
QUALIFY x = 1
SETTINGS query_plan_merge_expressions = 1, query_plan_execute_functions_after_sorting = 0, query_plan_optimize_lazy_materialization = 0, query_plan_merge_filter_into_join_condition = 0;

SELECT
  *
FROM
  t1__fuzz_0 RIGHT JOIN right_join__fuzz_0 USING (x)
QUALIFY x = 1;
