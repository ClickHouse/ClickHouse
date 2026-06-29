-- America/Paramaribo : partial hours timezones
select toDateTime(0, 'America/Paramaribo');
select toMonday(toDateTime(0, 'America/Paramaribo'));
select toStartOfWeek(toDateTime(0, 'America/Paramaribo'));
select toStartOfMonth(toDateTime(0, 'America/Paramaribo'));
select toStartOfQuarter(toDateTime(0, 'America/Paramaribo'));
select toStartOfYear(toDateTime(0, 'America/Paramaribo'));
select toTimeWithFixedDate(toDateTime(0, 'America/Paramaribo'), 'America/Paramaribo');
select toStartOfMinute(toDateTime(0, 'America/Paramaribo'));
select toStartOfFiveMinute(toDateTime(0, 'America/Paramaribo'));
select toStartOfTenMinutes(toDateTime(0, 'America/Paramaribo'));
select toStartOfFifteenMinutes(toDateTime(0, 'America/Paramaribo'));
select toStartOfHour(toDateTime(0, 'America/Paramaribo'));

-- Africa/Monrovia : partial minutes timezones
select toDateTime(0, 'Africa/Monrovia');
select toMonday(toDateTime(0, 'Africa/Monrovia'));
select toStartOfWeek(toDateTime(0, 'Africa/Monrovia'));
select toStartOfMonth(toDateTime(0, 'Africa/Monrovia'));
select toStartOfQuarter(toDateTime(0, 'Africa/Monrovia'));
select toStartOfYear(toDateTime(0, 'Africa/Monrovia'));
select toTimeWithFixedDate(toDateTime(0, 'Africa/Monrovia'), 'Africa/Monrovia');
select toStartOfMinute(toDateTime(0, 'Africa/Monrovia'));
select toStartOfFiveMinute(toDateTime(0, 'Africa/Monrovia'));
select toStartOfTenMinutes(toDateTime(0, 'Africa/Monrovia'));
select toStartOfFifteenMinutes(toDateTime(0, 'Africa/Monrovia'));
select toStartOfHour(toDateTime(0, 'Africa/Monrovia'));
