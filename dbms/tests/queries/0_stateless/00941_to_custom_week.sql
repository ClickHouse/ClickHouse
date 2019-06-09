-- week mode [0,7],  week test case. refer to the mysql test case
SELECT week(toDate('1998-01-01')), week(toDate('1997-01-01')),  week(toDate('1998-01-01'), 1),  week(toDate('1997-01-01'), 1);
SELECT week(toDate('1998-12-31')), week(toDate('1997-12-31')), week(toDate('1998-12-31'), 1), week(toDate('1997-12-31'), 1);
SELECT week(toDate('1995-01-01')), week(toDate('1995-01-01'), 1);
SELECT yearWeek(toDate('1981-12-31'), 1), yearWeek(toDate('1982-01-01'), 1), yearWeek(toDate('1982-12-31'), 1), yearWeek(toDate('1983-01-01'), 1);
SELECT yearWeek(toDate('1987-01-01'), 1), yearWeek(toDate('1987-01-01'));	
	
SELECT week(toDate('2000-01-01'),0) AS w2000, week(toDate('2001-01-01'),0) AS w2001, week(toDate('2002-01-01'),0) AS w2002,week(toDate('2003-01-01'),0) AS w2003, week(toDate('2004-01-01'),0) AS w2004, week(toDate('2005-01-01'),0) AS w2005, week(toDate('2006-01-01'),0) AS w2006;
SELECT week(toDate('2000-01-06'),0) AS w2000, week(toDate('2001-01-06'),0) AS w2001, week(toDate('2002-01-06'),0) AS w2002,week(toDate('2003-01-06'),0) AS w2003, week(toDate('2004-01-06'),0) AS w2004, week(toDate('2005-01-06'),0) AS w2005, week(toDate('2006-01-06'),0) AS w2006;
SELECT week(toDate('2000-01-01'),1) AS w2000, week(toDate('2001-01-01'),1) AS w2001, week(toDate('2002-01-01'),1) AS w2002,week(toDate('2003-01-01'),1) AS w2003, week(toDate('2004-01-01'),1) AS w2004, week(toDate('2005-01-01'),1) AS w2005, week(toDate('2006-01-01'),1) AS w2006;
SELECT week(toDate('2000-01-06'),1) AS w2000, week(toDate('2001-01-06'),1) AS w2001, week(toDate('2002-01-06'),1) AS w2002,week(toDate('2003-01-06'),1) AS w2003, week(toDate('2004-01-06'),1) AS w2004, week(toDate('2005-01-06'),1) AS w2005, week(toDate('2006-01-06'),1) AS w2006;
SELECT yearWeek(toDate('2000-01-01'),0) AS w2000, yearWeek(toDate('2001-01-01'),0) AS w2001, yearWeek(toDate('2002-01-01'),0) AS w2002,yearWeek(toDate('2003-01-01'),0) AS w2003, yearWeek(toDate('2004-01-01'),0) AS w2004, yearWeek(toDate('2005-01-01'),0) AS w2005, yearWeek(toDate('2006-01-01'),0) AS w2006;
SELECT yearWeek(toDate('2000-01-06'),0) AS w2000, yearWeek(toDate('2001-01-06'),0) AS w2001, yearWeek(toDate('2002-01-06'),0) AS w2002,yearWeek(toDate('2003-01-06'),0) AS w2003, yearWeek(toDate('2004-01-06'),0) AS w2004, yearWeek(toDate('2005-01-06'),0) AS w2005, yearWeek(toDate('2006-01-06'),0) AS w2006;
SELECT yearWeek(toDate('2000-01-01'),1) AS w2000, yearWeek(toDate('2001-01-01'),1) AS w2001, yearWeek(toDate('2002-01-01'),1) AS w2002,yearWeek(toDate('2003-01-01'),1) AS w2003, yearWeek(toDate('2004-01-01'),1) AS w2004, yearWeek(toDate('2005-01-01'),1) AS w2005, yearWeek(toDate('2006-01-01'),1) AS w2006;
SELECT yearWeek(toDate('2000-01-06'),1) AS w2000, yearWeek(toDate('2001-01-06'),1) AS w2001, yearWeek(toDate('2002-01-06'),1) AS w2002,yearWeek(toDate('2003-01-06'),1) AS w2003, yearWeek(toDate('2004-01-06'),1) AS w2004, yearWeek(toDate('2005-01-06'),1) AS w2005, yearWeek(toDate('2006-01-06'),1) AS w2006;	
SELECT week(toDate('1998-12-31'),2),week(toDate('1998-12-31'),3), week(toDate('2000-01-01'),2), week(toDate('2000-01-01'),3);
SELECT week(toDate('2000-12-31'),2),week(toDate('2000-12-31'),3);

SELECT week(toDate('1998-12-31'),0) AS w0, week(toDate('1998-12-31'),1) AS w1, week(toDate('1998-12-31'),2) AS w2, week(toDate('1998-12-31'),3) AS w3, week(toDate('1998-12-31'),4) AS w4, week(toDate('1998-12-31'),5) AS w5, week(toDate('1998-12-31'),6) AS w6, week(toDate('1998-12-31'),7) AS w7;
SELECT week(toDate('2000-01-01'),0) AS w0, week(toDate('2000-01-01'),1) AS w1, week(toDate('2000-01-01'),2) AS w2, week(toDate('2000-01-01'),3) AS w3, week(toDate('2000-01-01'),4) AS w4, week(toDate('2000-01-01'),5) AS w5, week(toDate('2000-01-01'),6) AS w6, week(toDate('2000-01-01'),7) AS w7;
SELECT week(toDate('2000-01-06'),0) AS w0, week(toDate('2000-01-06'),1) AS w1, week(toDate('2000-01-06'),2) AS w2, week(toDate('2000-01-06'),3) AS w3, week(toDate('2000-01-06'),4) AS w4, week(toDate('2000-01-06'),5) AS w5, week(toDate('2000-01-06'),6) AS w6, week(toDate('2000-01-06'),7) AS w7;
SELECT week(toDate('2000-12-31'),0) AS w0, week(toDate('2000-12-31'),1) AS w1, week(toDate('2000-12-31'),2) AS w2, week(toDate('2000-12-31'),3) AS w3, week(toDate('2000-12-31'),4) AS w4, week(toDate('2000-12-31'),5) AS w5, week(toDate('2000-12-31'),6) AS w6, week(toDate('2000-12-31'),7) AS w7;
SELECT week(toDate('2001-01-01'),0) AS w0, week(toDate('2001-01-01'),1) AS w1, week(toDate('2001-01-01'),2) AS w2, week(toDate('2001-01-01'),3) AS w3, week(toDate('2001-01-01'),4) AS w4, week(toDate('2001-01-01'),5) AS w5, week(toDate('2001-01-01'),6) AS w6, week(toDate('2001-01-01'),7) AS w7;

SELECT yearWeek(toDate('2000-12-31'),0), yearWeek(toDate('2000-12-31'),1), yearWeek(toDate('2000-12-31'),2), yearWeek(toDate('2000-12-31'),3), yearWeek(toDate('2000-12-31'),4), yearWeek(toDate('2000-12-31'),5), yearWeek(toDate('2000-12-31'),6), yearWeek(toDate('2000-12-31'),7);

-- week mode 8,9	
SELECT 
    toDate('2016-12-21') + number AS d, 
	week(d, 8) AS week8, 
    week(d, 9) AS week9, 
    yearWeek(d, 8) AS yearWeek8,
    yearWeek(d, 9) AS yearWeek9
FROM numbers(21);

SELECT toDateTime(toDate('2016-12-22') + number, 'Europe/Moscow' ) AS d, 
    week(d, 8, 'Europe/Moscow') AS week8, 
    week(d, 9, 'Europe/Moscow') AS week9, 
    yearWeek(d, 8, 'Europe/Moscow') AS yearWeek8,
    yearWeek(d, 9, 'Europe/Moscow') AS yearWeek9
FROM numbers(21);

