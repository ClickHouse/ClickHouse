DROP TABLE IF EXISTS fill;
CREATE TABLE fill (date Date, val Int, str String) ENGINE = Memory;
INSERT INTO fill VALUES (toDate('2019-05-24'), 13, 'sd0')(toDate('2019-05-10'), 16, 'vp7')(toDate('2019-05-25'), 17, '0ei')(toDate('2019-05-30'), 18, '3kd')(toDate('2019-05-15'), 27, 'enb')(toDate('2019-06-04'), 5, '6az')(toDate('2019-05-23'), 15, '01v')(toDate('2019-05-08'), 28, 'otf')(toDate('2019-05-19'), 20, 'yfh')(toDate('2019-05-07'), 26, '2ke')(toDate('2019-05-07'), 18, 'prh')(toDate('2019-05-09'), 25, '798')(toDate('2019-05-10'), 1, 'myj')(toDate('2019-05-11'), 18, '3s2')(toDate('2019-05-23'), 29, '72y');

SELECT '*** table without fill to compare ***';
SELECT * FROM fill ORDER BY date, val;

-- Some useful cases

SELECT '*** date WITH FILL, val ***';
SELECT * FROM fill ORDER BY date WITH FILL, val;

SELECT '*** date WITH FILL FROM 2019-05-01 TO 2019-05-31, val WITH FILL ***';
SELECT * FROM fill ORDER BY date WITH FILL FROM toDate('2019-05-01') TO toDate('2019-05-31'), val WITH FILL;

SELECT '*** date DESC WITH FILL, val WITH FILL FROM 1 TO 6 ***';
SELECT * FROM fill ORDER BY date DESC WITH FILL, val WITH FILL FROM 1 TO 6;

-- Some weird cases

SELECT '*** date DESC WITH FILL TO 2019-05-01 STEP -2, val DESC WITH FILL FROM 10 TO -5 STEP -3 ***';
SELECT * FROM fill ORDER BY date DESC WITH FILL TO toDate('2019-05-01') STEP -2, val DESC WITH FILL FROM 10 TO -5 STEP -3;

SELECT '*** date WITH FILL TO 2019-06-23 STEP 3, val WITH FILL FROM -10 STEP 2';
SELECT * FROM fill ORDER BY date WITH FILL TO toDate('2019-06-23') STEP 3, val WITH FILL FROM -10 STEP 2;

DROP TABLE fill;
CREATE TABLE fill (a UInt32, b Int32) ENGINE = Memory;
INSERT INTO fill VALUES (1, -2), (1, 3), (3, 2), (5, -1), (6, 5), (8, 0);

SELECT '*** table without fill to compare ***';
SELECT * FROM fill ORDER BY a, b;

SELECT '*** a WITH FILL, b WITH fill ***';
SELECT * FROM fill ORDER BY a WITH FILL, b WITH fill;

SELECT '*** a WITH FILL, b WITH fill TO 6 STEP 2 ***';
SELECT * FROM fill ORDER BY a WITH FILL, b WITH fill TO 6 STEP 2;

SELECT * FROM fill ORDER BY a WITH FILL STEP -1; -- { serverError 475 }
SELECT * FROM fill ORDER BY a WITH FILL FROM 10 TO 1; -- { serverError 475 }
SELECT * FROM fill ORDER BY a DESC WITH FILL FROM 1 TO 10; -- { serverError 475 }
SELECT * FROM fill ORDER BY a WITH FILL FROM -10 to 10; -- { serverError 475 }

DROP TABLE fill;
