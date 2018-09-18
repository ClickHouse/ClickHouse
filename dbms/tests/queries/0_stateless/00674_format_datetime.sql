select formatDateTime(toDateTime('2018-01-02 04:05:30', 'UTC'), '%c');
select formatDateTime(toDateTime(toDate('2018-01-02', 'UTC')), '%c');
select formatDateTime(toDateTime('2018-01-02 04:05:30', 'UTC'), '%F %T');
select formatDateTime(toDateTime('2018-01-02 04:05:30', 'UTC'), '%c %Z %z');
select formatDateTime(toTimeZone(toDateTime('2018-01-02 04:05:30', 'UTC'), 'America/Los_Angeles'), '%c %Z %z');
select formatDateTime(toTimeZone(toDateTime('2018-01-02 04:05:30', 'UTC'), 'Asia/Calcutta'), '%c %Z %z');
select formatDateTime(toTimeZone(toDateTime('2018-01-07 12:00:00', 'UTC'), 'America/Los_Angeles'), '%c');

select formatDateTime(t, '%x %X') from (select toTimeZone(toDateTime('2018-01-02 04:05:30', 'UTC') + number, 'UTC') as t from numbers(10));

drop temporary table if exists test;
create temporary table test(t DateTime('America/Los_Angeles'));
insert into test select toTimeZone(toDateTime('2018-03-05 14:34:36', 'UTC') + number, 'America/Los_Angeles') from numbers(10);
select formatDateTime(t, '%c %Z %z') from test;
drop temporary table test;
