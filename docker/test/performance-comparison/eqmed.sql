-- input is table(query text, run UInt32, version int, time float)
select
--   abs(diff_percent) > rd_quantiles_percent[3] fail,
   floor(original_medians_array.time_by_version[1], 4) l,
   floor(original_medians_array.time_by_version[2], 4) r,
   floor((r - l) / l, 3) diff_percent,
   arrayMap(x -> floor(x / l, 3), rd.rd_quantiles) rd_quantiles_percent,
   query 
from
   (
      select query, quantiles(0.05, 0.5, 0.95, 0.99)(abs(time_by_label[1] - time_by_label[2])) rd_quantiles -- quantiles of randomization distribution
      from
         (
            select query, virtual_run, groupArrayInsertAt(median_time, random_label) time_by_label -- make array 'random label' -> 'median time'
            from (
                  select query, medianExact(time) median_time, virtual_run, random_label -- get median times, grouping by random label
                  from (
                        select *, toUInt32(rowNumberInBlock() % 2) random_label -- randomly relabel measurements
                        from (
                              select query, time, number virtual_run 
                              from table, numbers(1, 10000) -- duplicate input measurements into many virtual runs
                              order by query, virtual_run, rand() -- for each virtual run, randomly reorder measurements
                           ) virtual_runs
                     ) relabeled 
                  group by query, virtual_run, random_label
               ) virtual_medians
            group by query, virtual_run -- aggregate by random_label
         ) virtual_medians_array
      group by query -- aggregate by virtual_run
   ) rd,
   (
        select groupArrayInsertAt(median_time, version) time_by_version, query
        from
           (
                select medianExact(time) median_time, query, version
                from table group by query, version
           ) original_medians
        group by query
   ) original_medians_array
where rd.query = original_medians_array.query
order by rd_quantiles_percent[3] desc;
