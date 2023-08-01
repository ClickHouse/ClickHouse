DROP TABLE IF EXISTS fh;

CREATE TABLE fh(a_value UInt32, b_value Float64, c_value Float64, d_value Float64) ENGINE = Memory;

INSERT INTO fh(a_value, b_value, c_value, d_value) VALUES (1, 5.6,-4.4, 2.6),(2, -9.6, 3, 3.3),(3, -1.3,-4, 1.2),(4, 5.3,9.7,2.3),(5, 4.4,0.037,1.222),(6, -8.6,-7.8,2.1233),(7, 5.1,9.3,8.1222),(8, 7.9,-3.6,9.837),(9, -8.2,0.62,8.43555),(10, -3,7.3,6.762);

SELECT corrMatrix(a_value) FROM (select a_value from fh limit 0);

SELECT corrMatrix(a_value) FROM (select a_value from fh limit 1);

SELECT corrMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 0);

SELECT corrMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 1);

SELECT arrayMap(x -> arrayMap(y -> round(y, 5), x), corrMatrix(a_value, b_value, c_value, d_value))  FROM fh;

SELECT round(abs(corr(x1,x2) - corrMatrix(x1,x2)[1][2]), 5), round(abs(corr(x1,x1) - corrMatrix(x1,x2)[1][1]), 5), round(abs(corr(x2,x2) - corrMatrix(x1,x2)[2][2]), 5) from (select randNormal(100, 1) as x1, randNormal(100,5) as x2 from numbers(100000));

SELECT covarSampMatrix(a_value) FROM (select a_value from fh limit 0);

SELECT covarSampMatrix(a_value) FROM (select a_value from fh limit 1);

SELECT covarSampMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 0);

SELECT covarSampMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 1);

SELECT arrayMap(x -> arrayMap(y -> round(y, 5), x), covarSampMatrix(a_value, b_value, c_value, d_value))  FROM fh;

SELECT round(abs(covarSamp(x1,x2) - covarSampMatrix(x1,x2)[1][2]), 5), round(abs(covarSamp(x1,x1) - covarSampMatrix(x1,x2)[1][1]), 5), round(abs(covarSamp(x2,x2) - covarSampMatrix(x1,x2)[2][2]), 5) from (select randNormal(100, 1) as x1, randNormal(100,5) as x2 from numbers(100000));

SELECT covarPopMatrix(a_value) FROM (select a_value from fh limit 0);

SELECT covarPopMatrix(a_value) FROM (select a_value from fh limit 1);

SELECT covarPopMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 0);

SELECT covarPopMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 1);

SELECT arrayMap(x -> arrayMap(y -> round(y, 5), x), covarPopMatrix(a_value, b_value, c_value, d_value))  FROM fh;

SELECT round(abs(covarPop(x1,x2) - covarPopMatrix(x1,x2)[1][2]), 5), round(abs(covarPop(x1,x1) - covarPopMatrix(x1,x2)[1][1]), 5), round(abs(covarPop(x2,x2) - covarPopMatrix(x1,x2)[2][2]), 5) from (select randNormal(100, 1) as x1, randNormal(100,5) as x2 from numbers(100000));
