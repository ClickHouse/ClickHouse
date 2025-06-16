-- Tags: no-fasttest
-- - no-fasttest -- requires S3

DROP TABLE IF EXISTS metric_log;

SYSTEM FLUSH LOGS metric_log;
CREATE TABLE metric_log AS system.metric_log
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (event_date, event_time)
SETTINGS disk = 's3_cache', merge_selector_base = 1000, min_bytes_for_wide_part = '1Gi';

insert into metric_log select * from generateRandom() limit 4e3;
insert into metric_log select * from generateRandom() limit 4e3;
insert into metric_log select * from generateRandom() limit 4e3;
insert into metric_log select * from generateRandom() limit 4e3;
insert into metric_log select * from generateRandom() limit 400;
insert into metric_log select * from generateRandom() limit 400;

OPTIMIZE TABLE metric_log FINAL;
SYSTEM FLUSH LOGS part_log;
select * from system.part_log where database = currentDatabase() and table = 'metric_log' and peak_memory_usage > 1_000_000_000 format Vertical;
