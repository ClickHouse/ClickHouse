SET send_logs_level = 'none';

select lcase('FOO');
select ucase('foo');
select LOWER('Foo');
select UPPER('Foo');
select REPLACE('bar', 'r', 'z');
select Locate('foo', 'o');
select SUBSTRING('foo', 1, 2);
select Substr('foo', 2);
select mid('foo', 3);
select IF(3>2, 1, 0);
select substring('foo' from 1 + 1);
select SUBSTRING('foo' FROM 2 FOR 1);
