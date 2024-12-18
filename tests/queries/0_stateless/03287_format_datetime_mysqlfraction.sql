-- %f (default settings)
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 8, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=0;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 6, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=0;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 4, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=0;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 2, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=0;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 1, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=0;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 0, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=0;
-- %f (setting before issue #72879 fix - printing the digits of the scale only)
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 8, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=1;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 6, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=1;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 4, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=1;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 2, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=1;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 1, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=1;
select formatDateTime(toDateTime64('1970-01-01 00:00:00.123456', 0, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_only=1;