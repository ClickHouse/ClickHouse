select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 8, 'UTC'), '%f');
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 6, 'UTC'), '%f');
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 4, 'UTC'), '%f');
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 2, 'UTC'), '%f');
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 1, 'UTC'), '%f');
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 0, 'UTC'), '%f');
