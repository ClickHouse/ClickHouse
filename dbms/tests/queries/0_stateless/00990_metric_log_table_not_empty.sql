select sleep(2) format Null; --More than collect_interval_milliseconds

system flush logs;

select count()>0 from system.metric_log
