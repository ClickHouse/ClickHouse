select sleep(2) format Null;

system flush logs;

select count()>0 from system.metric_log