SELECT formatDateTime(toDate('2010-01-01'), '%G'); -- Friday (first day of the year) attributed to week 53 of the previous year (2009)
SELECT formatDateTime(toDate('2010-01-01'), '%g');
SELECT formatDateTime(toDate('2010-01-03'), '%G'); -- Sunday, last day attributed to week 53 of the previous year (2009)
SELECT formatDateTime(toDate('2010-01-03'), '%g');
SELECT formatDateTime(toDate('2010-01-04'), '%G'); -- Monday, first day in the year attributed to week 01 of the current year (2010)
SELECT formatDateTime(toDate('2010-01-04'), '%g');
SELECT formatDateTime(toDate('2018-12-31'), '%G'); -- Monday (last day of the year) attributed to 01 week of next year (2019)
SELECT formatDateTime(toDate('2018-12-31'), '%g');
SELECT formatDateTime(toDate('2019-01-01'), '%G'); -- Tuesday (first day of the year) attributed to 01 week of this year (2019)
SELECT formatDateTime(toDate('2019-01-01'), '%g');
