select 'year', age('year', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'year', age('year', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'quarter', age('quarter', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'quarter', age('quarter', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'month', age('month', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'month', age('month', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'week', age('week', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'week', age('week', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'day', age('day', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'day', age('day', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'minute', age('minute', toDate32('1969-12-31'), toDate32('1970-01-01'));

select 'second', age('second', toDate32('1969-12-31'), toDate32('1970-01-01'));
