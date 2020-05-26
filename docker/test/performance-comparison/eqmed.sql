-- input is table(test text, query text, run UInt32, version int, metrics Array(float))
select
   arrayMap(x -> floor(x, 4), original_medians_array.medians_by_version[1] as l) l_rounded,
   arrayMap(x -> floor(x, 4), original_medians_array.medians_by_version[2] as r) r_rounded,
   arrayMap(x, y -> floor((y - x) / x, 3), l, r) diff_percent,
   arrayMap(x, y -> floor(x / y, 3), threshold, l) threshold_percent,
   test, query
from
   (
      -- quantiles of randomization distributions
      select quantileExactForEach(0.999)(
        arrayMap(x, y -> abs(x - y), metrics_by_label[1], metrics_by_label[2]) as d
      ) threshold
      ---- uncomment to see what the distribution is really like
      --, uniqExact(d.1) u
      --, arraySort(x->x.1,
      --      arrayZip(
      --          (sumMap([d.1], [1]) as f).1,
      --          f.2)) full_histogram
      from
         (
            -- make array 'random label' -> '[median metric]'
            select virtual_run, groupArrayInsertAt(median_metrics, random_label) metrics_by_label
            from (
                  -- get [median metric] arrays among virtual runs, grouping by random label
                  select medianExactForEach(metrics) median_metrics, virtual_run, random_label
                  from (
                        -- randomly relabel measurements
                        select *, toUInt32(rowNumberInAllBlocks() % 2) random_label
                        from (
                              select metrics, number virtual_run
                              from
                                -- strip the query away before the join -- it might be several kB long;
                                (select metrics, run, version from table) no_query,
                                -- duplicate input measurements into many virtual runs
                                numbers(1, 100000) nn
                              -- for each virtual run, randomly reorder measurements
                              order by virtual_run, rand()
                           ) virtual_runs
                     ) relabeled 
                  group by virtual_run, random_label
               ) virtual_medians
            group by virtual_run -- aggregate by random_label
         ) virtual_medians_array
      -- this select aggregates by virtual_run
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
       select throwIf(uniq((test, query)) != 1) from table
   ) check_single_query -- this subselect checks that there is only one query in the input table;
                        -- written this way so that it is not optimized away (#10523)
;
