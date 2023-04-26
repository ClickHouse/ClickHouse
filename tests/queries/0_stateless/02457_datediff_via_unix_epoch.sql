select 'year', date_diff('year', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'year', date_diff('year', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'quarter', date_diff('quarter', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'quarter', date_diff('quarter', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'month', date_diff('month', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'month', date_diff('month', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'week', date_diff('week', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'week', date_diff('week', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'day', date_diff('day', toDate32('1969-12-25'), toDate32('1970-01-05'));
select 'day', date_diff('day', toDateTime64('1969-12-25 10:00:00.000', 3), toDateTime64('1970-01-05 10:00:00.000', 3));

select 'minute', date_diff('minute', toDate32('1969-12-31'), toDate32('1970-01-01'));

select 'second', date_diff('second', toDate32('1969-12-31'), toDate32('1970-01-01'));
