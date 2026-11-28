-- A `parallel_full_sorting_merge` join scatters both sides into a fixed number of shards and pairs those
-- shards positionally, so a side that merges its shards back into one stream leaves the two sides with a
-- different number of output ports. The join then falls back to the plain pipeline, which used to accept
-- only one port per side and raised `LOGICAL_ERROR: Join is supported only for pipelines with one output
-- port`. Below, the `LIMIT` side is budgeted one thread and merges back while the `UNION ALL` side keeps
-- its shards.

-- The merged side has to reach the join whole and in join-key order, so the result must match the hash
-- join, which does not use this pipeline.
SELECT
    (SELECT (count(), sum(cityHash64(l.number, r.number)))
     FROM (SELECT number FROM system.numbers LIMIT 100) AS l
     INNER JOIN (SELECT number FROM system.numbers LIMIT 100 UNION ALL SELECT number FROM system.numbers LIMIT 100) AS r
         ON l.number = r.number
     SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (count(), sum(cityHash64(l.number, r.number)))
     FROM (SELECT number FROM system.numbers LIMIT 100) AS l
     INNER JOIN (SELECT number FROM system.numbers LIMIT 100 UNION ALL SELECT number FROM system.numbers LIMIT 100) AS r
         ON l.number = r.number
     SETTINGS join_algorithm = 'hash');
