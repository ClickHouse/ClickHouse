SELECT count(number), 1 AS k1, 2 as k2, 3 as k3 FROM numbers_mt(10000000) GROUP BY k1, k2, k3 settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions=0;
SELECT count(number), 1 AS k1, 2 as k2, 3 as k3 FROM numbers_mt(10000000) GROUP BY k1, k2, k3 settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions = 0;
SELECT count(number), 1 AS k1, 2 as k2, 3 as k3 FROM numbers_mt(10000000) GROUP BY k1, k2, k3 settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=0, compile_aggregate_expressions = 1;
SELECT count(number), 1 AS k1, 2 as k2, 3 as k3 FROM numbers_mt(10000000) GROUP BY k1, k2, k3 settings optimize_group_by_constant_keys=1, enable_software_prefetch_in_aggregation=1, compile_aggregate_expressions = 1;

