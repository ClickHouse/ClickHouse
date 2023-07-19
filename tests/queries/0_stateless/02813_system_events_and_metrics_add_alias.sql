show create table system.metrics;
select equals((select count() from system.metrics where name=metric) as r1, (select count() from system.metrics) as r2);
show create table system.events;
select equals((select count() from system.events where name=event) as r1, (select count() from system.events) as r2);