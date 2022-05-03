drop table if exists data_02233;
create table data_02233 (partition Int, parent_key Int, child_key Int, value Int) engine=MergeTree() partition by partition order by parent_key;

insert into data_02233 values (1, 10, 100, 1000)(1, 20, 200, 2000);
insert into data_02233 values (2, 10, 100, 1000)(2, 20, 200, 2000);

-- { echoOn }
explain pipeline select groupArraySorted(partition), parent_key, child_key, sum(value) from data_02233 group by parent_key, child_key with totals order by parent_key, child_key settings max_threads=1, optimize_aggregation_in_order=1;
explain pipeline select groupArraySorted(partition), parent_key, child_key, sum(value) from data_02233 group by parent_key, child_key with totals order by parent_key, child_key settings max_threads=1;
select groupArraySorted(partition), parent_key, child_key, sum(value) from data_02233 group by parent_key, child_key with totals order by parent_key, child_key settings optimize_aggregation_in_order=1;
select groupArraySorted(partition), parent_key, child_key, sum(value) from data_02233 group by parent_key, child_key with totals order by parent_key, child_key settings optimize_aggregation_in_order=1, max_block_size=1;
-- sum() can be compiled, check that compiled version works correctly
select groupArraySorted(partition), parent_key, child_key, sum(value) from data_02233 group by parent_key, child_key with totals order by parent_key, child_key settings optimize_aggregation_in_order=1, compile_aggregate_expressions=1, min_count_to_compile_aggregate_expression=0;
select groupArraySorted(partition), parent_key, child_key, sum(value) from data_02233 group by parent_key, child_key with totals order by parent_key, child_key;

-- fuzzer
SELECT child_key, parent_key, child_key FROM data_02233 GROUP BY parent_key, child_key, child_key ORDER BY child_key, parent_key ASC NULLS LAST SETTINGS max_threads = 1, optimize_aggregation_in_order = 1;
SELECT child_key, parent_key, child_key FROM data_02233 GROUP BY parent_key, child_key, child_key WITH TOTALS ORDER BY child_key, parent_key ASC NULLS LAST SETTINGS max_threads = 1, optimize_aggregation_in_order = 1;

-- { echoOff }
drop table data_02233;
