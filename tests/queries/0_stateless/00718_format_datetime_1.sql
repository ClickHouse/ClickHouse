select formatDateTime(toDateTime64('1900-01-01 00:00:00.000', 3, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('1962-12-08 18:11:29.123', 3, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('1970-01-01 00:00:00.001', 3, 'UTC'), '%F %T.%f');
