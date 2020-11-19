set multiple_joins_rewriter_version = 2;

select
    arrayMap(x, y -> floor((y - x) / x, 3), l, r) diff_percent,
    test, query
from (select [1] l) s1,
    (select [2] r) s2,
    (select 'test' test, 'query' query) any_query,
    (select 1 ) check_single_query;

select
    arrayMap(x -> floor(x, 4), original_medians_array.medians_by_version[1] as l) l_rounded,
    arrayMap(x -> floor(x, 4), original_medians_array.medians_by_version[2] as r) r_rounded,
    arrayMap(x, y -> floor((y - x) / x, 3), l, r) diff_percent,
    test, query
from (select 1) rd,
    (select [[1,2], [3,4]] medians_by_version) original_medians_array,
    (select 'test' test, 'query' query) any_query,
    (select 1 as A) check_single_query;

drop table if exists table;
create table table(query String, test String, run UInt32, metrics Array(UInt32), version UInt32) engine Memory;

select
    arrayMap(x -> floor(x, 4), original_medians_array.medians_by_version[1] as l) l_rounded,
    arrayMap(x -> floor(x, 4), original_medians_array.medians_by_version[2] as r) r_rounded,
    arrayMap(x, y -> floor((y - x) / x, 3), l, r) diff_percent,
    arrayMap(x, y -> floor(x / y, 3), threshold, l) threshold_percent,
    test, query
from
(
    select quantileExactForEach(0.999)(arrayMap(x, y -> abs(x - y), metrics_by_label[1], metrics_by_label[2]) as d) threshold
    from
    (
        select virtual_run, groupArrayInsertAt(median_metrics, random_label) metrics_by_label
        from
        (
            select medianExactForEach(metrics) median_metrics, virtual_run, random_label
            from
            (
                select *, toUInt32(rowNumberInAllBlocks() % 2) random_label
                from
                (
                    select metrics, number virtual_run
                    from (select metrics, run, version from table) no_query, numbers(1, 100000) nn
                    order by virtual_run, rand()
                ) virtual_runs
            ) relabeled 
            group by virtual_run, random_label
        ) virtual_medians
        group by virtual_run
    ) virtual_medians_array
) rd,
(
    select groupArrayInsertAt(median_metrics, version) medians_by_version
    from
    (
        select medianExactForEach(metrics) median_metrics, version
        from table
        group by version
    ) original_medians
) original_medians_array,
(
    select any(test) test, any(query) query from table
) any_query,
(
    select throwIf(uniq((test, query))) from table
) check_single_query;

drop table table;
