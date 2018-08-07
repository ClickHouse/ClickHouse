select formatDateTime(toDateTime('2018-01-02 04:05:30'), '%c');
select formatDateTime(toDateTime(toDate('2018-01-02')), '%c');
select formatDateTime(toDateTime('2018-01-02 04:05:30'), '%F %T');
select formatDateTime(toDateTime('2018-01-02 04:05:30'), '%c %Z %z');
select formatDateTime(toTimeZone(toDateTime('2018-01-02 04:05:30'), 'America/Los_Angeles'), '%c %Z %z');
select formatDateTime(toTimeZone(toDateTime('2018-01-02 04:05:30'), 'Asia/Calcutta'), '%c %Z %z');
select formatDateTime(toTimeZone(toDateTime('2018-01-02 04:05:30'), 'Asia/Kathmandu'), '%c %Z %z');

select formatDateTime(t, '%x %X') from (select toDateTime('2018-01-02 04:05:30') + number as t from numbers(10));

drop temporary table if exists test;
create temporary table test(t DateTime('America/Los_Angeles'));
insert into test select toDateTime('2018-03-05 14:34:36') + number from numbers(10);
select formatDateTime(t, '%c %Z %z') from test;
drop temporary table test;
