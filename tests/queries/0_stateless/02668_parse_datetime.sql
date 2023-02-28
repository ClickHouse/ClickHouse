-- { echoOn }
-- year
select parseDateTime('2020', '%Y') = toDateTime('2020-01-01');

-- month
select parseDateTime('02', '%m') = toDateTime('1970-02-01');
select parseDateTime('07', '%m') = toDateTime('1970-07-01');
select parseDateTime('11-', '%m-') = toDateTime('1970-11-01');
select parseDateTime('02', '%c') = toDateTime('1970-01-01');
select parseDateTime('jun', '%b') = toDateTime('1970-06-01');
select parseDateTime('02', '%m'); -- { serverError LOGICAL_ERROR }

-- day of month
select parseDateTime('07', '%d') = toDateTime('2020-01-07');
select parseDateTime('01', '%d') = toDateTime('2020-01-01');
select parseDateTime('/11', '/%d') = toDateTime('2020-01-11');
select parseDateTime('32', '%d'); -- { serverError LOGICAL_ERROR }
select parseDateTime('02-31', '%m-%d'); -- { serverError LOGICAL_ERROR }
select parseDateTime('04-31', '%m-%d'); -- { serverError LOGICAL_ERROR }
select parseDateTime('2000-02-29', '%Y-%m-%d') = toDateTime('2000-02-29');
select parseDateTime('2001-02-29', '%Y-%m-%d'); -- { serverError LOGICAL_ERROR }

-- day of year
select parseDateTime('001', '%j') = toDateTime('2000-01-01');
select parseDateTime('007', '%j') = toDateTime('2000-01-07');
select parseDateTime('/031/', '/%j/') = toDateTime('2000-01-31');
select parseDateTime('032', '%j') = toDateTime('2000-02-01');
select parseDateTime('060', '%j') = toDateTime('2000-02-29');
select parseDateTime('365', '%j') = toDateTime('2000-12-30');
select parseDateTime('366', '%j') = toDateTime('2000-12-31');
select parseDateTime('1980 001', '%Y %j') = toDateTime('1980-01-01');
select parseDateTime('1980 007', '%Y %j') = toDateTime('1980-01-07');
select parseDateTime('1980 /007', '%Y /%j') = toDateTime('1980-01-11');
select parseDateTime('1980 /031/', '%Y /%j/') = toDateTime('1980-01-31');
select parseDateTime('1980 032', '%Y %j') = toDateTime('1980-02-01');
select parseDateTime('1980 060', '%Y %j') = toDateTime('1980-02-29');
select parseDateTime('1980 366', '%Y %j') = toDateTime('1980-12-31');
select parseDateTime('367', '%j'); -- { serverError LOGICAL_ERROR }
select parseDateTime('000', '%j'); -- { serverError LOGICAL_ERROR }

-- hour of day
select parseDateTime('07', '%H') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('23', '%H') = toDateTime('1970-01-01 23:00:00');
select parseDateTime('00', '%H') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('10', '%H') = toDateTime('1970-01-01 10:00:00');
select parseDateTime('24', '%H'); -- { serverError LOGICAL_ERROR }
select parseDateTime('-1', '%H'); -- { serverError LOGICAL_ERROR }
select parseDateTime('1234567', '%H'); -- { serverError LOGICAL_ERROR }
select parseDateTime('07', '%k') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('23', '%k') = toDateTime('1970-01-01 23:00:00');
select parseDateTime('00', '%k') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('10', '%k') = toDateTime('1970-01-01 10:00:00');
select parseDateTime('24', '%k'); -- { serverError LOGICAL_ERROR }
select parseDateTime('-1', '%k'); -- { serverError LOGICAL_ERROR }
select parseDateTime('1234567', '%k'); -- { serverError LOGICAL_ERROR }

-- clock hour of half day
select parseDateTime('07', '%h') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('12', '%h') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('01', '%h') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('10', '%h') = toDateTime('1970-01-01 10:00:00');
select parseDateTime('00', '%h'); -- { serverError LOGICAL_ERROR }
select parseDateTime('13', '%h'); -- { serverError LOGICAL_ERROR }
select parseDateTime('123456789', '%h'); -- { serverError LOGICAL_ERROR }
select parseDateTime('07', '%I') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('12', '%I') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('01', '%I') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('10', '%I') = toDateTime('1970-01-01 10:00:00');
select parseDateTime('00', '%I'); -- { serverError LOGICAL_ERROR }
select parseDateTime('13', '%I'); -- { serverError LOGICAL_ERROR }
select parseDateTime('123456789', '%I'); -- { serverError LOGICAL_ERROR }
select parseDateTime('07', '%l') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('12', '%l') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('01', '%l') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('10', '%l') = toDateTime('1970-01-01 10:00:00');
select parseDateTime('00', '%l'); -- { serverError LOGICAL_ERROR }
select parseDateTime('13', '%l'); -- { serverError LOGICAL_ERROR }
select parseDateTime('123456789', '%l'); -- { serverError LOGICAL_ERROR }

-- half of day
select parseDateTime('07 PM', '%H %p') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('07 AM', '%H %p') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('07 pm', '%H %p') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('07 am', '%H %p') = toDateTime('1970-01-01 07:00:00');
select parseDateTime('00 AM', '%H %p') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('00 PM', '%H %p') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('00 am', '%H %p') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('00 pm', '%H %p') = toDateTime('1970-01-01 00:00:00');

select parseDateTime('01 PM', '%h %p') = toDateTime('1970-01-01 13:00:00');
select parseDateTime('01 AM', '%h %p') = toDateTime('1970-01-01 01:00:00');
select parseDateTime('06 PM', '%h %p') = toDateTime('1970-01-01 18:00:00');
select parseDateTime('06 AM', '%h %p') = toDateTime('1970-01-01 06:00:00');
select parseDateTime('12 PM', '%h %p') = toDateTime('1970-01-01 12:00:00');
select parseDateTime('12 AM', '%h %p') = toDateTime('1970-01-01 00:00:00');

-- minute
select parseDateTime('08', '%i') = toDateTime('1970-01-01 00:08:00');
select parseDateTime('59', '%i') = toDateTime('1970-01-01 00:59:00');
select parseDateTime('00/', '%i/') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('60', '%i'); -- { serverError LOGICAL_ERROR }
select parseDateTime('-1', '%i'); -- { serverError LOGICAL_ERROR }
select parseDateTime('123456789', '%i'); -- { serverError LOGICAL_ERROR }

-- second
select parseDateTime('09', '%s') = toDateTime('1970-01-01 00:00:09');
select parseDateTime('58', '%s') = toDateTime('1970-01-01 00:00:58');
select parseDateTime('00/', '%s/') = toDateTime('1970-01-01 00:00:00');
select parseDateTime('60', '%s'); -- { serverError LOGICAL_ERROR }
select parseDateTime('-1', '%s'); -- { serverError LOGICAL_ERROR }
select parseDateTime('123456789', '%s'); -- { serverError LOGICAL_ERROR }

-- mixed YMD format
select parseDateTime('2021-01-04+23:00:00', '%Y-%m-%d+%H:%i:%s') = toDateTime('2021-01-04 23:00:00');
select parseDateTime('2019-07-03 11:04:10', '%Y-%m-%d %H:%i:%s') = toDateTime('2019-07-03 11:04:10');
select parseDateTime('10:04:11 03-07-2019', '%s:%i:%H %d-%m-%Y') = toDateTime('2019-07-03 11:04:10');

-- { echoOff }
