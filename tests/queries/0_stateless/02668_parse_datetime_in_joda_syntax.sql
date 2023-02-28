-- empty
select parseDateTimeInJodaSyntax(' ', ' ', 'UTC') = toDateTime('1970-01-01', 'UTC');

-- era
select parseDateTimeInJodaSyntax('AD 1999', 'G YYYY') = toDateTime('1999-01-01');
select parseDateTimeInJodaSyntax('ad 1999', 'G YYYY') = toDateTime('1999-01-01');
select parseDateTimeInJodaSyntax('Ad 1999', 'G YYYY') = toDateTime('1999-01-01');
select parseDateTimeInJodaSyntax('AD 1999', 'G YYYY') = toDateTime('1999-01-01');
select parseDateTimeInJodaSyntax('AD 1999', 'G yyyy') = toDateTime('1999-01-01');
select parseDateTimeInJodaSyntax('AD 1999 2000', 'G YYYY yyyy') = toDateTime('2000-01-01');
select parseDateTimeInJodaSyntax('AD 1999 2000', 'G yyyy YYYY') = toDateTime('2000-01-01');
select parseDateTimeInJodaSyntax('AD 1999', 'G Y'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('AD 1999', 'G YY'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('AD 1999', 'G YYY'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('BC', 'G'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('AB', 'G'); -- { serverError LOGICAL_ERROR }

-- year of era
select parseDateTimeInJodaSyntax('2106', 'YYYY', 'UTC') = toDateTime('2106-01-01', 'UTC');
select parseDateTimeInJodaSyntax('1970', 'YYYY', 'UTC') = toDateTime('1970-01-01', 'UTC');
select parseDateTimeInJodaSyntax('1969', 'YYYY', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('2107', 'YYYY', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('+1999', 'YYYY', 'UTC') -- { serverError LOGICAL_ERROR }

select parseDateTimeInJodaSyntax('12', 'YY', 'UTC') = toDateTime('2012-01-01', 'UTC');
select parseDateTimeInJodaSyntax('69', 'YY', 'UTC') = toDateTime('2069-01-01', 'UTC');
select parseDateTimeInJodaSyntax('70', 'YY', 'UTC') = toDateTime('1970-01-01', 'UTC');
select parseDateTimeInJodaSyntax('99', 'YY', 'UTC') = toDateTime('1999-01-01', 'UTC');
select parseDateTimeInJodaSyntax('01', 'YY', 'UTC') = toDateTime('2001-01-01', 'UTC');
select parseDateTimeInJodaSyntax('1', 'YY', 'UTC'); -- { serverError LOGICAL_ERROR }

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
select parseDateTimeInJodaSyntax('1969', 'yyyy', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('2107', 'yyyy', 'UTC'); -- { serverError LOGICAL_ERROR }

-- week year
select parseDateTimeInJodaSyntax('2106', 'xxxx', 'UTC') = toDateTime('2106-01-04', 'UTC');
select parseDateTimeInJodaSyntax('1971', 'xxxx', 'UTC') = toDateTime('1971-01-04', 'UTC');
select parseDateTimeInJodaSyntax('2025', 'xxxx', 'UTC') = toDateTime('2024-12-30', 'UTC');
select parseDateTimeInJodaSyntax('12', 'xx', 'UTC') = toDateTime('2012-01-02', 'UTC');
select parseDateTimeInJodaSyntax('69', 'xx', 'UTC') = toDateTime('2068-12-31', 'UTC');
select parseDateTimeInJodaSyntax('99', 'xx', 'UTC') = toDateTime('1999-01-04', 'UTC');
select parseDateTimeInJodaSyntax('01', 'xx', 'UTC') = toDateTime('2001-01-01', 'UTC');
select parseDateTimeInJodaSyntax('+10', 'xx', 'UTC') = toDateTime('2001-01-04', 'UTC');
select parseDateTimeInJodaSyntax('+99 01', 'xx ww', 'UTC') = toDateTime('1999-01-04', 'UTC');
select parseDateTimeInJodaSyntax('+99 02', 'xx ww', 'UTC') = toDateTime('1999-01-11', 'UTC');
select parseDateTimeInJodaSyntax('10 +10', 'ww xx', 'UTC') = toDateTime('2010-03-08', 'UTC');
select parseDateTimeInJodaSyntax('2+10', 'wwxx', 'UTC') = toDateTime('2010-01-11', 'UTC');
select parseDateTimeInJodaSyntax('+102', 'xxM', 'UTC') = toDateTime('2010-02-01', 'UTC');
select parseDateTimeInJodaSyntax('+20102', 'xxxxM', 'UTC') = toDateTime('2010-02-01', 'UTC');
select parseDateTimeInJodaSyntax('1970', 'xxxx', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('1969', 'xxxx', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('2107', 'xxxx', 'UTC'); -- { serverError LOGICAL_ERROR }

-- century of era
select parseDateTimeInJodaSyntax('20', 'CC', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax('21', 'CC', 'UTC') = toDateTime('2100-01-01', 'UTC');
select parseDateTimeInJodaSyntax('19', 'CC', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('22', 'CC', 'UTC'); -- { serverError LOGICAL_ERROR }

-- month
select parseDateTimeInJodaSyntax('1', 'M', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax(' 7', ' MM', 'UTC') = toDateTime('2000-07-01', 'UTC');
select parseDateTimeInJodaSyntax('11', 'M', 'UTC') = toDateTime('2000-11-01', 'UTC');
select parseDateTimeInJodaSyntax('10-', 'M-', 'UTC') = toDateTime('2000-10-01', 'UTC');
select parseDateTimeInJodaSyntax('-12-', '-M-', 'UTC') = toDateTime('2000-12-01', 'UTC');
select parseDateTimeInJodaSyntax('0', 'M', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('13', 'M', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('12345', 'M', 'UTC'); -- { serverError LOGICAL_ERROR }
--- Ensure MMM and MMMM specifiers consume both short- and long-form month names
select parseDateTimeInJodaSyntax('Aug', 'MMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('AuG', 'MMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('august', 'MMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('Aug', 'MMMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('AuG', 'MMMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
select parseDateTimeInJodaSyntax('august', 'MMMM', 'UTC') = toDateTime('2000-08-01', 'UTC');
--- invalid month names
select parseDateTimeInJodaSyntax('Decembr', 'MMM', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('Decembr', 'MMMM', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('Decemberary', 'MMM', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('Decemberary', 'MMMM', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('asdf', 'MMM', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('asdf', 'MMMM', 'UTC'); -- { serverError LOGICAL_ERROR }

-- day of month
select parseDateTimeInJodaSyntax('1', 'd', 'UTC') = toDateTime('2000-01-01', 'UTC');
select parseDateTimeInJodaSyntax('7 ', 'dd ', 'UTC') = toDateTime('2000-01-07', 'UTC');
select parseDateTimeInJodaSyntax('/11', '/dd', 'UTC') = toDateTime('2000-01-11', 'UTC');
select parseDateTimeInJodaSyntax('/31/', '/d/', 'UTC') = toDateTime('2000-01-31', 'UTC');
select parseDateTimeInJodaSyntax('0', 'd', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('32', 'd', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('12345', 'd', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('02-31', 'M-d', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('04-31', 'M-d', 'UTC'); -- { serverError LOGICAL_ERROR }
--- Ensure all days of month are checked against final selected month
select parseDateTimeInJodaSyntax('2 31 1', 'M d M') = toDateTime('2000-01-31', 'UTC');
select parseDateTimeInJodaSyntax('1 31 20 2', 'M d d M'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('2 31 20 4', 'M d d M'); -- { serverError LOGICAL_ERROR }
--- Leap year
select parseDateTimeInJodaSyntax('2020-02-29', 'YYYY-M-d') = toDateTime('2020-02-29', 'UTC');
select parseDateTimeInJodaSyntax('2001-02-29', 'YYYY-M-d'); -- { serverError LOGICAL_ERROR }

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
select parseDateTimeInJodaSyntax('1999 366', 'yyyy D', 'UTC'); -- { serverError LOGICAL_ERROR }
--- Ensure all days of year are checked against final selected year
select parseDateTimeInJodaSyntax('2001 366 2000', 'yyyy D yyyy', 'UTC') = toDateTime('2000-12-31', 'UTC');
select parseDateTimeInJodaSyntax('2000 366 2001', 'yyyy D yyyy', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('0', 'D', 'UTC'); -- { serverError LOGICAL_ERROR }
select parseDateTimeInJodaSyntax('367', 'D', 'UTC'); -- { serverError LOGICAL_ERROR }

