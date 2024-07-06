CREATE TABLE t_02967
(
  `key` Date,
  `value` UInt16
)
ENGINE = MergeTree
ORDER BY key
SETTINGS
  index_granularity_bytes = 0 --8192 --, min_index_granularity_bytes = 2  
 , index_granularity = 100
 , min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0
--
--  , min_bytes_for_wide_part = 2
AS SELECT
  number,
  repeat(toString(number), 5)
FROM numbers(105.);



-- Check with newly inserted data part. It's in-memory structured are filled at insert time.
SELECT
  count(ignore(*))
FROM t_02967
PREWHERE CAST(ignore() + 1 as UInt8)
GROUP BY 
  ignore(65535, *),
  ignore(255, 256, *)
SETTINGS
 --send_logs_level='test', 
 max_threads=1;



-- Reload part form disk to check that in-meory structures where properly serilaized-deserialized
DETACH TABLE t_02967;
ATTACH TABLE t_02967;


SELECT
  count(ignore(*))
FROM t_02967
PREWHERE CAST(ignore() + 1 as UInt8)
GROUP BY 
  ignore(65535, *),
  ignore(255, 256, *)
SETTINGS
 --send_logs_level='test', 
 max_threads=1;

DROP TABLE t_02967;
