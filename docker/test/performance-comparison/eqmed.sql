-- input is table(query text, run UInt32, version int, time float)
select
   floor(original_medians_array.time_by_version[1], 4) l,
   floor(original_medians_array.time_by_version[2], 4) r,
   floor((r - l) / l, 3) diff_percent,
   floor(threshold / l, 3) threshold_percent,
   query
from
   (
      -- quantiles of randomization distributions
      select quantileExact(0.999)(abs(time_by_label[1] - time_by_label[2]) as d) threshold
      ---- uncomment to see what the distribution is really like
      --, uniqExact(d) u
      --, arraySort(x->x.1,
      --      arrayZip(
      --          (sumMap([d], [1]) as f).1,
      --          f.2)) full_histogram
      from
         (
            select virtual_run, groupArrayInsertAt(median_time, random_label) time_by_label -- make array 'random label' -> 'median time'
            from (
                  select medianExact(time) median_time, virtual_run, random_label -- get median times, grouping by random label
                  from (
                        select *, toUInt32(rowNumberInAllBlocks() % 2) random_label -- randomly relabel measurements
                        from (
                              select time, number virtual_run 
                              from
                                -- strip the query away before the join -- it might be several kB long;
                                (select time, run, version from table) no_query,
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
        select groupArrayInsertAt(median_time, version) time_by_version
        from
        (
            select medianExact(time) median_time, version
            from table
            group by version
        ) original_medians
   ) original_medians_array,
   (
        select any(query) query from table
   ) any_query,
   (
       select throwIf(uniq(query) != 1) from table
   ) check_single_query -- this subselect checks that there is only one query in the input table;
                        -- written this way so that it is not optimized away (#10523)
;
