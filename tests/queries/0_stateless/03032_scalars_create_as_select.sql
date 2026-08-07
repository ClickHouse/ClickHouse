create table query_run_metric_arrays engine Memory as with (with (select groupUniqArrayArray(['a', 'b']) from numbers(1)) as all_names select all_names) as all_metrics select all_metrics;
select * from query_run_metric_arrays;
