select interval 1 second, interval 1 minute, interval 1 hour;
select interval 1 day, interval 1 week, interval 1 month;
select interval 1 quarter, interval 1 year;

select date '2001-09-29';
select (date '2001-09-29' + interval 7 day) x, toTypeName(x);
select (date '2001-10-01' - interval 7 day) x, toTypeName(x);
select (date '2001-09-29' + 7) x, toTypeName(x);
select (date '2001-10-01' - 7) x, toTypeName(x);
select (date '2001-09-29' + interval 1 hour) x, toTypeName(x);
select (date '2001-09-29' - interval 1 hour) x, toTypeName(x);
select (date '2001-10-01' - date '2001-09-28') x, toTypeName(x);
select timestamp '2001-09-28 01:00:00' + interval 23 hour;
select timestamp '2001-09-28 23:00:00' - interval 23 hour;

SET session_timezone = 'Europe/Amsterdam';

select (date '2001-09-29' + interval 12345 second) x, toTypeName(x);
select (date '2001-09-29' + interval 12345 millisecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (date '2001-09-29' + interval 12345 microsecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (date '2001-09-29' + interval 12345 nanosecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (date '2001-09-29' - interval 12345 second) x, toTypeName(x);
select (date '2001-09-29' - interval 12345 millisecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (date '2001-09-29' - interval 12345 microsecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (date '2001-09-29' - interval 12345 nanosecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (toDate32('2001-09-29') + interval 12345 second) x, toTypeName(x);
select (toDate32('2001-09-29') + interval 12345 millisecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (toDate32('2001-09-29') + interval 12345 microsecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (toDate32('2001-09-29') + interval 12345 nanosecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (toDate32('2001-09-29') - interval 12345 second) x, toTypeName(x);
select (toDate32('2001-09-29') - interval 12345 millisecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (toDate32('2001-09-29') - interval 12345 microsecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select (toDate32('2001-09-29') - interval 12345 nanosecond) x, toTypeName(x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select (timestamp '2001-12-29 03:00:00' - timestamp '2001-12-27 12:00:00') x, toTypeName(x);

select -interval 23 hour;
select interval 1 day + interval 1 hour;
select interval '1 day' - interval '1 hour';

-- select date '2001-09-28' + time '03:00';
-- select time '01:00' + interval '3 hours';
-- select time '05:00' - time '03:00';
-- select time '05:00' - interval '2 hours';

-- select 900 * interval '1 second'; -- interval '00:15:00'
-- select (21 * interval '1 day') x, toTypeName(x); -- interval '21 days'
-- select (double precision '3.5' * interval '1 hour') x, toTypeName(x); -- interval '03:30:00'
-- select (interval '1 hour' / double precision '1.5') x, toTypeName(x); -- interval '00:40:00'
