select interval 1 second, interval 1 minute, interval 1 hour;
select interval 1 day, interval 1 week, interval 1 month;
select interval 1 quarter, interval 1 year;

select date '2001-09-28';
select (date '2001-09-28' + interval 7 day) x, toTypeName(x);
select (date '2001-10-01' - interval 7 day) x, toTypeName(x);
select (date '2001-09-28' + 7) x, toTypeName(x);
select (date '2001-10-01' - 7) x, toTypeName(x);
select (date '2001-09-28' + interval 1 hour) x, toTypeName(x);
select (date '2001-09-28' - interval 1 hour) x, toTypeName(x);
select (date '2001-10-01' - date '2001-09-28') x, toTypeName(x);
select timestamp '2001-09-28 01:00:00' + interval 23 hour;
select timestamp '2001-09-28 23:00:00' - interval 23 hour;

-- TODO: return interval
select (timestamp '2001-09-29 03:00:00' - timestamp '2001-09-27 12:00:00') x, toTypeName(x); -- interval '1 day 15:00:00'

-- select -interval 23 hour; -- interval '-23:00:00'
-- select interval 1 day + interval 1 hour; -- interval '1 day 01:00:00'
-- select interval '1 day' - interval '1 hour'; -- interval '1 day -01:00:00'

-- select date '2001-09-28' + time '03:00'; -- timestamp '2001-09-28 03:00:00'
-- select time '01:00' + interval '3 hours'; -- time '04:00:00'
-- select time '05:00' - time '03:00'; -- interval '02:00:00'
-- select time '05:00' - interval '2 hours'; -- time '03:00:00'

-- select 900 * interval '1 second'; -- interval '00:15:00'
-- select (21 * interval '1 day') x, toTypeName(x); -- interval '21 days'
-- select (double precision '3.5' * interval '1 hour') x, toTypeName(x); -- interval '03:30:00'
-- select (interval '1 hour' / double precision '1.5') x, toTypeName(x); -- interval '00:40:00'
