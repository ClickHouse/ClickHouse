drop table if exists test;
-- this queries does not have to pass, but they works historically
-- let's support this while can, see #31687
create table test (x String) Engine=StripeLog;
insert into test values (0);
select if(0, y, 42) from test;
select if(1, 42, y) from test;
select if(toUInt8(0), y, 42) from test;
select if(toInt8(0), y, 42) from test;
select if(toUInt8(1), 42, y) from test;
select if(toInt8(1), 42, y) from test;
select if(toUInt8(toUInt8(0)), y, 42) from test;
select if(cast(cast(0, 'UInt8'), 'UInt8'), y, 42) from test;
explain syntax select x, if((select hasColumnInTable(currentDatabase(), 'test', 'y')), y, x || '_')  from test;
drop table if exists t;
