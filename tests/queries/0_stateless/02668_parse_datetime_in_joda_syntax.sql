-- { echoOn }
-- empty
select parseDateTimeInJodaSyntax(' ', ' ', 'UTC') = toDateTime('1970-01-01', 'UTC');

-- era
select parseDateTimeInJodaSyntax('AD 1999', 'G YYYY', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('ad 1999', 'G YYYY', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('Ad 1999', 'G YYYY', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('AD 1999', 'G yyyy', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('AD 1999 2000', 'G YYYY yyyy', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax('AD 1999 2000', 'G yyyy YYYY', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax('AD 1999', 'G Y'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('AD 1999', 'G YY'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('AD 1999', 'G YYY', 'UTC');
select parseDateTimeInJodaSyntax('BC', 'G'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('AB', 'G'); -- { serverError CANNOT_PARSE_DATETIME }

-- year of era
select parseDateTimeInJodaSyntax('2106', 'YYYY', 'UTC') = toDateTime('2106-01-01', 'UTC');
select parseDateTimeInJodaSyntax('1970', 'YYYY', 'UTC') = toDateTime('1970-01-01', 'UTC');
select parseDateTimeInJodaSyntax('1969', 'YYYY', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('2107', 'YYYY', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('+1999', 'YYYY', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

select parseDateTimeInJodaSyntax('12', 'YY', 'UTC') = toDateTime('2012-01-01', 'UTC');
select parseDateTimeInJodaSyntax('69', 'YY', 'UTC') = toDateTime('2069-01-01', 'UTC');
select parseDateTimeInJodaSyntax('70', 'YY', 'UTC') = toDateTime('1970-01-01', 'UTC');
select parseDateTimeInJodaSyntax('99', 'YY', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('01', 'YY', 'UTC') = toDateTime('2001-01-01', 'UTC');
select parseDateTimeInJodaSyntax('1', 'YY', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

select parseDateTimeInJodaSyntax('99 98 97', 'YY YY YY', 'UTC') = toDateTime('1997-01-01', 'UTC');

-- year
select parseDateTimeInJodaSyntax('12', 'yy', 'UTC') = toDateTime('2012-01-01', 'UTC');
select parseDateTimeInJodaSyntax('69', 'yy', 'UTC') = toDateTime('2069-01-01', 'UTC');
select parseDateTimeInJodaSyntax('70', 'yy', 'UTC') = toDateTime('1970-01-01', 'UTC');
select parseDateTimeInJodaSyntax('99', 'yy', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('+99', 'yy', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('+99 02', 'yy MM', 'UTC') = toDateTime('1999-02-01', 'UTC');
select parseDateTimeInJodaSyntax('10 +10', 'MM yy', 'UTC') = toDateTime('2010-10-01', 'UTC');
select parseDateTimeInJodaSyntax('10+2001', 'MMyyyy', 'UTC') = toDateTime('2001-10-01', 'UTC');
select parseDateTimeInJodaSyntax('+200110', 'yyyyMM', 'UTC') = toDateTime('2001-10-01', 'UTC');
select parseDateTimeInJodaSyntax('1970', 'yyyy', 'UTC') = toDateTime('1970-01-01', 'UTC');
select parseDateTimeInJodaSyntax('2106', 'yyyy', 'UTC') = toDateTime('2106-01-01', 'UTC');
select parseDateTimeInJodaSyntax('1969', 'yyyy', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('2107', 'yyyy', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- week year
select parseDateTimeInJodaSyntax('2106', 'xxxx', 'UTC') = toDateTime('2106-01-04', 'UTC');
select parseDateTimeInJodaSyntax('1971', 'xxxx', 'UTC') = toDateTime('1971-01-04', 'UTC');
select parseDateTimeInJodaSyntax('2025', 'xxxx', 'UTC') = toDateTime('2024-12-30', 'UTC');
select parseDateTimeInJodaSyntax('12', 'xx', 'UTC') = toDateTime('2012-01-02', 'UTC');
select parseDateTimeInJodaSyntax('69', 'xx', 'UTC') = toDateTime('2068-12-31', 'UTC');
select parseDateTimeInJodaSyntax('99', 'xx', 'UTC') = toDateTime('1999-01-04', 'UTC');
select parseDateTimeInJodaSyntax('01', 'xx', 'UTC') = toDateTime('2001-01-01', 'UTC');
select parseDateTimeInJodaSyntax('+10', 'xx', 'UTC') = toDateTime('2010-01-04', 'UTC');
select parseDateTimeInJodaSyntax('+99 01', 'xx ww', 'UTC') = toDateTime('1999-01-04', 'UTC');
select parseDateTimeInJodaSyntax('+99 02', 'xx ww', 'UTC') = toDateTime('1999-01-11', 'UTC');
select parseDateTimeInJodaSyntax('10 +10', 'ww xx', 'UTC') = toDateTime('2010-03-08', 'UTC');
select parseDateTimeInJodaSyntax('2+10', 'wwxx', 'UTC') = toDateTime('2010-01-11', 'UTC');
select parseDateTimeInJodaSyntax('+102', 'xxM', 'UTC') = toDateTime('2010-02-01', 'UTC');
select parseDateTimeInJodaSyntax('+20102', 'xxxxM', 'UTC') = toDateTime('2010-02-01', 'UTC');
select parseDateTimeInJodaSyntax('1970', 'xxxx', 'UTC'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
select parseDateTimeInJodaSyntax('1969', 'xxxx', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('2107', 'xxxx', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- century of era
select parseDateTimeInJodaSyntax('20', 'CC', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax('21', 'CC', 'UTC') = toDateTime('2100-01-01', 'UTC');
select parseDateTimeInJodaSyntax('19', 'CC', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('22', 'CC', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- month
select parseDateTimeInJodaSyntax('1', 'M', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax(' 7', ' MM', 'UTC') = toDateTime('2000-07-01', 'UTC');
select parseDateTimeInJodaSyntax('11', 'M', 'UTC') = toDateTime('2000-11-01', 'UTC');
select parseDateTimeInJodaSyntax('10-', 'M-', 'UTC') = toDateTime('2000-10-01', 'UTC');
select parseDateTimeInJodaSyntax('-12-', '-M-', 'UTC') = toDateTime('2000-12-01', 'UTC');
select parseDateTimeInJodaSyntax('0', 'M', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('13', 'M', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('12345', 'M', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
--- Ensure MMM and MMMM specifiers consume both short- and long-form month names
select parseDateTimeInJodaSyntax('Aug', 'MMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('AuG', 'MMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('august', 'MMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('Aug', 'MMMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('AuG', 'MMMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('august', 'MMMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
--- invalid month names
select parseDateTimeInJodaSyntax('Decembr', 'MMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('Decembr', 'MMMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('Decemberary', 'MMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('Decemberary', 'MMMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('asdf', 'MMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('asdf', 'MMMM', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- day of month
select parseDateTimeInJodaSyntax('1', 'd', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax('7 ', 'dd ', 'UTC') = toDateTime('2000-01-07', 'UTC');
select parseDateTimeInJodaSyntax('/11', '/dd', 'UTC') = toDateTime('2000-01-11', 'UTC');
select parseDateTimeInJodaSyntax('/31/', '/d/', 'UTC') = toDateTime('2000-01-31', 'UTC');
select parseDateTimeInJodaSyntax('0', 'd', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('32', 'd', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('12345', 'd', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('02-31', 'M-d', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('04-31', 'M-d', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
-- The last one is chosen if multiple day of months are supplied.
select parseDateTimeInJodaSyntax('2 31 1', 'M d M', 'UTC') = toDateTime('2000-01-31', 'UTC');
select parseDateTimeInJodaSyntax('1 31 20 2', 'M d d M', 'UTC') = toDateTime('2000-02-20', 'UTC');
select parseDateTimeInJodaSyntax('2 31 20 4', 'M d d M', 'UTC') = toDateTime('2000-04-20', 'UTC');
--- Leap year
select parseDateTimeInJodaSyntax('2020-02-29', 'YYYY-M-d', 'UTC') = toDateTime('2020-02-29', 'UTC');
select parseDateTimeInJodaSyntax('2001-02-29', 'YYYY-M-d', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- day of year
select parseDateTimeInJodaSyntax('1', 'D', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax('7 ', 'DD ', 'UTC') = toDateTime('2000-01-07', 'UTC');
select parseDateTimeInJodaSyntax('/11', '/DD', 'UTC') = toDateTime('2000-01-11', 'UTC');
select parseDateTimeInJodaSyntax('/31/', '/DDD/', 'UTC') = toDateTime('2000-01-31', 'UTC');
select parseDateTimeInJodaSyntax('32', 'D', 'UTC') = toDateTime('2000-02-01', 'UTC');
select parseDateTimeInJodaSyntax('60', 'D', 'UTC') = toDateTime('2000-02-29', 'UTC');
select parseDateTimeInJodaSyntax('365', 'D', 'UTC') = toDateTime('2000-12-30', 'UTC');
select parseDateTimeInJodaSyntax('366', 'D', 'UTC') = toDateTime('2000-12-31', 'UTC');
select parseDateTimeInJodaSyntax('1999 1', 'yyyy D', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('1999 7 ', 'yyyy DD ', 'UTC') = toDateTime('1999-01-07', 'UTC');
select parseDateTimeInJodaSyntax('1999 /11', 'yyyy /DD', 'UTC') = toDateTime('1999-01-11', 'UTC');
select parseDateTimeInJodaSyntax('1999 /31/', 'yyyy /DD/', 'UTC') = toDateTime('1999-01-31', 'UTC');
select parseDateTimeInJodaSyntax('1999 32', 'yyyy D', 'UTC') = toDateTime('1999-02-01', 'UTC');
select parseDateTimeInJodaSyntax('1999 60', 'yyyy D', 'UTC') = toDateTime('1999-03-01', 'UTC');
select parseDateTimeInJodaSyntax('1999 365', 'yyyy D', 'UTC') = toDateTime('1999-12-31', 'UTC');
select parseDateTimeInJodaSyntax('1999 366', 'yyyy D', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
--- Ensure all days of year are checked against final selected year
select parseDateTimeInJodaSyntax('2001 366 2000', 'yyyy D yyyy', 'UTC') = toDateTime('2000-12-31', 'UTC');
select parseDateTimeInJodaSyntax('2000 366 2001', 'yyyy D yyyy', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('0', 'D', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('367', 'D', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- hour of day
select parseDateTimeInJodaSyntax('7', 'H', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('23', 'HH', 'UTC') = toDateTime('1970-01-01 23:00:00', 'UTC');
select parseDateTimeInJodaSyntax('0', 'HHH', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('10', 'HHHHHHHH', 'UTC') = toDateTime('1970-01-01 10:00:00', 'UTC');
--- invalid hour od day
select parseDateTimeInJodaSyntax('24', 'H', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('-1', 'H', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('123456789', 'H', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- clock hour of day
select parseDateTimeInJodaSyntax('7', 'k', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('24', 'kk', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('1', 'kkk', 'UTC') = toDateTime('1970-01-01 01:00:00', 'UTC');
select parseDateTimeInJodaSyntax('10', 'kkkkkkkk', 'UTC') = toDateTime('1970-01-01 10:00:00', 'UTC');
-- invalid clock hour of day
select parseDateTimeInJodaSyntax('25', 'k', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('0', 'k', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('123456789', 'k', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- hour of half day
select parseDateTimeInJodaSyntax('7', 'K', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('11', 'KK', 'UTC') = toDateTime('1970-01-01 11:00:00', 'UTC');
select parseDateTimeInJodaSyntax('0', 'KKK', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('10', 'KKKKKKKK', 'UTC') = toDateTime('1970-01-01 10:00:00', 'UTC');
-- invalid hour of half day
select parseDateTimeInJodaSyntax('12', 'K', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('-1', 'K', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('123456789', 'K', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- clock hour of half day
select parseDateTimeInJodaSyntax('7', 'h', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('12', 'hh', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('1', 'hhh', 'UTC') = toDateTime('1970-01-01 01:00:00', 'UTC');
select parseDateTimeInJodaSyntax('10', 'hhhhhhhh', 'UTC') = toDateTime('1970-01-01 10:00:00', 'UTC');
-- invalid clock hour of half day
select parseDateTimeInJodaSyntax('13', 'h', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('0', 'h', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('123456789', 'h', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- half of day
--- Half of day has no effect if hour or clockhour of day is provided hour of day tests
select parseDateTimeInJodaSyntax('7 PM', 'H a', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('7 AM', 'H a', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('7 pm', 'H a', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('7 am', 'H a', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('0 PM', 'H a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('0 AM', 'H a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('0 pm', 'H a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('0 am', 'H a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('7 PM', 'k a', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('7 AM', 'k a', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('7 pm', 'k a', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('7 am', 'k a', 'UTC') = toDateTime('1970-01-01 07:00:00', 'UTC');
select parseDateTimeInJodaSyntax('24 PM', 'k a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('24 AM', 'k a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('24 pm', 'k a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('24 am', 'k a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
-- Half of day has effect if hour or clockhour of halfday is provided
select parseDateTimeInJodaSyntax('0 PM', 'K a', 'UTC') = toDateTime('1970-01-01 12:00:00', 'UTC');
select parseDateTimeInJodaSyntax('0 AM', 'K a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('6 PM', 'K a', 'UTC') = toDateTime('1970-01-01 18:00:00', 'UTC');
select parseDateTimeInJodaSyntax('6 AM', 'K a', 'UTC') = toDateTime('1970-01-01 06:00:00', 'UTC');
select parseDateTimeInJodaSyntax('11 PM', 'K a', 'UTC') = toDateTime('1970-01-01 23:00:00', 'UTC');
select parseDateTimeInJodaSyntax('11 AM', 'K a', 'UTC') = toDateTime('1970-01-01 11:00:00', 'UTC');
select parseDateTimeInJodaSyntax('1 PM', 'h a', 'UTC') = toDateTime('1970-01-01 13:00:00', 'UTC');
select parseDateTimeInJodaSyntax('1 AM', 'h a', 'UTC') = toDateTime('1970-01-01 01:00:00', 'UTC');
select parseDateTimeInJodaSyntax('6 PM', 'h a', 'UTC') = toDateTime('1970-01-01 18:00:00', 'UTC');
select parseDateTimeInJodaSyntax('6 AM', 'h a', 'UTC') = toDateTime('1970-01-01 06:00:00', 'UTC');
select parseDateTimeInJodaSyntax('12 PM', 'h a', 'UTC') = toDateTime('1970-01-01 12:00:00', 'UTC');
select parseDateTimeInJodaSyntax('12 AM', 'h a', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
-- time gives precendent to most recent time specifier
select parseDateTimeInJodaSyntax('0 1 AM', 'H h a', 'UTC') = toDateTime('1970-01-01 01:00:00', 'UTC');
select parseDateTimeInJodaSyntax('12 1 PM', 'H h a', 'UTC') = toDateTime('1970-01-01 13:00:00', 'UTC');
select parseDateTimeInJodaSyntax('1 AM 0', 'h a H', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('1 AM 12', 'h a H', 'UTC') = toDateTime('1970-01-01 12:00:00', 'UTC');

-- minute
select parseDateTimeInJodaSyntax('8', 'm', 'UTC') = toDateTime('1970-01-01 00:08:00', 'UTC');
select parseDateTimeInJodaSyntax('59', 'mm', 'UTC') = toDateTime('1970-01-01 00:59:00', 'UTC');
select parseDateTimeInJodaSyntax('0/', 'mmm/', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('60', 'm', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('-1', 'm', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('123456789', 'm', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- second
select parseDateTimeInJodaSyntax('9', 's', 'UTC') = toDateTime('1970-01-01 00:00:09', 'UTC');
select parseDateTimeInJodaSyntax('58', 'ss', 'UTC') = toDateTime('1970-01-01 00:00:58', 'UTC');
select parseDateTimeInJodaSyntax('0/', 's/', 'UTC') = toDateTime('1970-01-01 00:00:00', 'UTC');
select parseDateTimeInJodaSyntax('60', 's', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('-1', 's', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('123456789', 's', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- integer overflow in AST Fuzzer
select parseDateTimeInJodaSyntax('19191919191919191919191919191919', 'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- *OrZero, *OrNull
select parseDateTimeInJodaSyntaxOrZero('2001 366 2000', 'yyyy D yyyy', 'UTC') = toDateTime('2000-12-31', 'UTC');
select parseDateTimeInJodaSyntaxOrZero('2001 invalid 366 2000', 'yyyy D yyyy', 'UTC') = toDateTime('1970-01-01', 'UTC');
select parseDateTimeInJodaSyntaxOrNull('2001 366 2000', 'yyyy D yyyy', 'UTC') = toDateTime('2000-12-31', 'UTC');
select parseDateTimeInJodaSyntaxOrNull('2001 invalid 366 2000', 'yyyy D yyyy', 'UTC') IS NULL;

-- Error handling
select parseDateTimeInJodaSyntax(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select parseDateTimeInJodaSyntax('12 AM', 'h a', 'UTC', 'a fourth argument'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The format string argument is optional
set session_timezone = 'UTC'; -- don't randomize the session timezone
select parseDateTimeInJodaSyntax('2021-01-04 23:12:34') = toDateTime('2021-01-04 23:12:34');
select parseDateTimeInJodaSyntax('2024-10-09 10:30:10-0812'); -- { serverError CANNOT_PARSE_DATETIME }

-- timezone and timezone offset
select parseDateTimeInJodaSyntax('2024-10-09 10:30:10-0812', 'yyyy-MM-dd HH:mm:ssZ') = toDateTime64('2024-10-09 18:42:10', 6);
select parseDateTimeInJodaSyntax('2024-10-09 10:30:10-08123', 'yyyy-MM-dd HH:mm:ssZZZ'); -- {serverError CANNOT_PARSE_DATETIME}
select parseDateTimeInJodaSyntax('2024-10-09 10:30:10EST', 'yyyy-MM-dd HH:mm:ssz') = toDateTime64('2024-10-09 15:30:10', 6);
select parseDateTimeInJodaSyntax('2024-10-09 10:30:10EST', 'yyyy-MM-dd HH:mm:sszzz') = toDateTime64('2024-10-09 15:30:10', 6);
-- incorrect timezone offset and timezone
select parseDateTimeInJodaSyntax('2024-10-09 10:30:10-8000', 'yyyy-MM-dd HH:mm:ssZ'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeInJodaSyntax('2024-10-09 10:30:10ABCD', 'yyyy-MM-dd HH:mm:ssz'); -- { serverError BAD_ARGUMENTS }

-- -------------------------------------------------------------------------------------------------------------------------
-- Tests for parseDateTime64InJodaSyntax, these are not systematic

select parseDateTime64InJodaSyntax('', '') = toDateTime64('1970-01-01 00:00:00', 0);
select parseDateTime64InJodaSyntax('2177-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');
select parseDateTime64InJodaSyntax('+0000', 'Z') = toDateTime64('1970-01-01 00:00:00', 0);
select parseDateTime64InJodaSyntax('08:01', 'HH:ss') = toDateTime64('1970-01-01 08:00:01', 0);
select parseDateTime64InJodaSyntax('2024-01-02', 'yyyy-MM-dd') = toDateTime64('2024-01-02 00:00:00', 0);
select parseDateTime64InJodaSyntax('10:30:50', 'HH:mm:ss') = toDateTime64('1970-01-01 10:30:50', 0);
select parseDateTime64InJodaSyntax('2024-12-31 23:30:10.123456-0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ') = toDateTime64('2025-01-01 07:30:10.123456', 6);
select parseDateTime64InJodaSyntax('2024-01-01 00:00:01.123456+0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ') = toDateTime64('2023-12-31 16:00:01.123456', 6);
select parseDateTime64InJodaSyntax('2021-01-04 23:12:34') = toDateTime64('2021-01-04 23:12:34', 0);
select parseDateTime64InJodaSyntax('2021-01-04 23:12:34.331', 'yyyy-MM-dd HH:mm:ss.SSS') = toDateTime64('2021-01-04 23:12:34.331', 3);
select parseDateTime64InJodaSyntax('2021/01/04 23:12:34.331', 'yyyy/MM/dd HH:mm:ss.SSS') = toDateTime64('2021-01-04 23:12:34.331', 3);
select parseDateTime64InJodaSyntax('2021-01-04 23:12:34.331'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2021-01-04 23:12:34.331', 'yyyy-MM-dd HH:mm:ss.SSSS') = toDateTime64('2021-01-04 23:12:34.0331', 4);
select parseDateTime64InJodaSyntax('2021-01-04 23:12:34.331', 'yyyy-MM-dd HH:mm:ss.SS'); -- { serverError CANNOT_PARSE_DATETIME }

-- Timezone and timezone offset
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10-0812'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-0812', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ') = toDateTime64('2024-10-09 18:42:10.123456', 6);
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-08123', 'yyyy-MM-dd HH:mm:ss.SSSSSSZZZ'); -- {serverError CANNOT_PARSE_DATETIME}
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456EST', 'yyyy-MM-dd HH:mm:ss.SSSSSSz') = toDateTime64('2024-10-09 15:30:10.123456', 6);
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456EST', 'yyyy-MM-dd HH:mm:ss.SSSSSSzzz') = toDateTime64('2024-10-09 15:30:10.123456', 6);
select parseDateTime64InJodaSyntax('2024-11-05-0800 01:02:03.123456', 'yyyy-MM-ddZ HH:mm:ss.SSSSSS') = toDateTime64('2024-11-05 09:02:03.123456', 6);
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456America/Los_Angeles', 'yyyy-MM-dd HH:mm:ss.SSSSSSz') = toDateTime64('2024-10-09 17:30:10.123456', 6);
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456Australia/Adelaide', 'yyyy-MM-dd HH:mm:ss.SSSSSSz') = toDateTime64('2024-10-09 00:00:10.123456', 6);
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 'yyyy-dd-MM HH:mm:ss.SSS') = toDateTime64('2024-09-10 10:30:10.123', 3);
select parseDateTime64InJodaSyntax('999999 10-09-202410:30:10', 'SSSSSSSSS dd-MM-yyyyHH:mm:ss'); -- {serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-0845', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ') = toDateTime64('2024-10-09 19:15:10.123456', 6);

-- incorrect timezone offset and timezone
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-8000', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456ABCD', 'yyyy-MM-dd HH:mm:ss.SSSSSSz'); -- { serverError BAD_ARGUMENTS }
select parseDateTime64InJodaSyntax('2023-02-29 11:22:33Not/Timezone', 'yyyy-MM-dd HH:mm:ssz'); -- { serverError BAD_ARGUMENTS }

-- leap vs non-leap years
select parseDateTime64InJodaSyntax('2024-02-29 11:23:34America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz') = toDateTime64('2024-02-29 19:23:34', 0);
select parseDateTime64InJodaSyntax('2023-02-29 11:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2024-02-28 23:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz') = toDateTime64('2024-02-29 07:22:33', 0);
select parseDateTime64InJodaSyntax('2023-02-28 23:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz') = toDateTime64('2023-03-01 07:22:33', 0);
select parseDateTime64InJodaSyntax('2024-03-01 00:22:33-8000', 'yyyy-MM-dd HH:mm:ssZ'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2023-03-01 00:22:33-8000', 'yyyy-MM-dd HH:mm:ssZ'); -- { serverError CANNOT_PARSE_DATETIME }

-- parseDateTime64InJodaSyntaxOrNull
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS') = toDateTime64('2024-10-09 10:30:10.123', 3);
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') = toDateTime64('2024-10-09 10:30:10.123456', 6);
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456-0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ') = toDateTime64('2024-10-09 18:30:10.123456', 6);
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456America/Los_Angeles', 'yyyy-MM-dd HH:mm:ss.SSSSSSz') = toDateTime64('2024-10-09 17:30:10.123456', 6);
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 'yyyy-dd-MM HH:mm:ss.SSS') = toDateTime64('2024-09-10 10:30:10.123', 3);
select parseDateTime64InJodaSyntaxOrNull('2023-02-29 11:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz') is NULL;
select parseDateTime64InJodaSyntaxOrNull('', '') = toDateTime64('1970-01-01 00:00:00', 0);
select parseDateTime64InJodaSyntaxOrNull('2177-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');

-- parseDateTime64InJodaSyntaxOrZero
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS') = toDateTime64('2024-10-09 10:30:10.123', 3);
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') = toDateTime64('2024-10-09 10:30:10.123456', 6);
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456-0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ') = toDateTime64('2024-10-09 18:30:10.123456', 6);
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456America/Los_Angeles', 'yyyy-MM-dd HH:mm:ss.SSSSSSz') = toDateTime64('2024-10-09 17:30:10.123456', 6);
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 'yyyy-dd-MM HH:mm:ss.SSS') = toDateTime64('2024-09-10 10:30:10.123', 3);
select parseDateTime64InJodaSyntaxOrZero('wrong value', 'yyyy-dd-MM HH:mm:ss.SSS') = toDateTime64('1970-01-01 00:00:00.000', 3);
select parseDateTime64InJodaSyntaxOrZero('2023-02-29 11:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz') = toDateTime64('1970-01-01 00:00:00', 0);
select parseDateTime64InJodaSyntaxOrZero('', '') = toDateTime64('1970-01-01 00:00:00', 0);
select parseDateTime64InJodaSyntaxOrZero('2177-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS') = toDateTime64('2177-10-09 10:30:10.123', 3);

-- Test parseDataTime64InJodaSyntax supports range between 1900 - 2299
select parseDateTime64InJodaSyntax('1899-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('1900-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') = toDateTime64('1900-01-01 00:00:00', 0);
select parseDateTime64InJodaSyntax('1920-06-06 00:00:01', 'yyyy-MM-dd HH:mm:ss') = toDateTime64('1920-06-06 00:00:01', 0);
select parseDateTime64InJodaSyntax('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') = toDateTime64('1970-01-01 00:00:00', 0);
select parseDateTime64InJodaSyntax('1971-02-03 04:05:06', 'yyyy-MM-dd HH:mm:ss') = toDateTime64('1971-02-03 04:05:06', 0);
select parseDateTime64InJodaSyntax('2105-02-03 04:05:06', 'yyyy-MM-dd HH:mm:ss') = toDateTime64('2105-02-03 04:05:06', 0);
select parseDateTime64InJodaSyntax('2106-02-07 06:28:15', 'yyyy-MM-dd HH:mm:ss') = toDateTime64('2106-02-07 06:28:15', 0);
select parseDateTime64InJodaSyntax('2106-02-07 16:28:15', 'yyyy-MM-dd HH:mm:ss') = toDateTime64('2106-02-07 16:28:15', 0);
select parseDateTime64InJodaSyntax('2299-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') = toDateTime64('2299-12-31 23:59:59', 0);
select parseDateTime64InJodaSyntax('2300-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss'); -- { serverError CANNOT_PARSE_DATETIME }

-- Test parseDateTimeInJodaSyntax with 3 repetitions in format and 4 digits year
select parseDateTimeInJodaSyntax('2025', 'YYY', 'UTC') = toDateTime('2025-01-01', 'UTC');
select parseDateTimeInJodaSyntax('2025', 'xxx', 'UTC') = toDateTime('2024-12-30', 'UTC');
select parseDateTimeInJodaSyntax('2025', 'yyy', 'UTC') = toDateTime('2025-01-01', 'UTC');

-- -------------------------------------------------------------------------------------------------------------------------

-- { echoOff }
