SELECT serverUUID() AS s, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY s format Null;

select getMacro('replica') as s, count() from remote('127.0.0.{1,2}', system.one) group by s;

select uptime() as s, count() FROM remote('127.0.0.{1,2}', system.one) group by s format Null;
