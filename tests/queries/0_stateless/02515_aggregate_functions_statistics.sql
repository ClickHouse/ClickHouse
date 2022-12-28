DROP TABLE IF EXISTS fh;

CREATE TABLE fh(a_value UInt32, b_value Float64, c_value Float64, d_value Float64) ENGINE = Memory;

INSERT INTO fh(a_value, b_value, c_value, d_value) VALUES (1, 5.6,-4.4, 2.6),(2, -9.6, 3, 3.3),(3, -1.3,-4, 1.2),(4, 5.3,9.7,2.3),(5, 4.4,0.037,1.222),(6, -8.6,-7.8,2.1233),(7, 5.1,9.3,8.1222),(8, 7.9,-3.6,9.837),(9, -8.2,0.62,8.43555),(10, -3,7.3,6.762);

SELECT corrMatrix(a_value) FROM (select a_value from fh limit 0);

SELECT corrMatrix(a_value) FROM (select a_value from fh limit 1);

SELECT corrMatrix(a_value, b_value) FROM (select a_value, b_value from fh limit 0);

SELECT corrMatrix(a_value, b_value) FROM (select a_value, b_value from fh limit 1);

SELECT corrMatrix(a_value, b_value, c_value) FROM (select a_value, b_value, c_value from fh limit 0);

SELECT corrMatrix(a_value, b_value, c_value) FROM (select a_value, b_value, c_value from fh limit 1);

SELECT corrMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 0);

SELECT corrMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 1);


SELECT covarSampMatrix(a_value) FROM (select a_value from fh limit 0);

SELECT covarSampMatrix(a_value) FROM (select a_value from fh limit 1);

SELECT covarSampMatrix(a_value, b_value) FROM (select a_value, b_value from fh limit 0);

SELECT covarSampMatrix(a_value, b_value) FROM (select a_value, b_value from fh limit 1);

SELECT covarSampMatrix(a_value, b_value, c_value) FROM (select a_value, b_value, c_value from fh limit 0);

SELECT covarSampMatrix(a_value, b_value, c_value) FROM (select a_value, b_value, c_value from fh limit 1);

SELECT covarSampMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 0);

SELECT covarSampMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 1);


SELECT covarPopMatrix(a_value) FROM (select a_value from fh limit 0);

SELECT covarPopMatrix(a_value) FROM (select a_value from fh limit 1);

SELECT covarPopMatrix(a_value, b_value) FROM (select a_value, b_value from fh limit 0);

SELECT covarPopMatrix(a_value, b_value) FROM (select a_value, b_value from fh limit 1);

SELECT covarPopMatrix(a_value, b_value, c_value) FROM (select a_value, b_value, c_value from fh limit 0);

SELECT covarPopMatrix(a_value, b_value, c_value) FROM (select a_value, b_value, c_value from fh limit 1);

SELECT covarPopMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 0);

SELECT covarPopMatrix(a_value, b_value, c_value, d_value) FROM (select a_value, b_value, c_value, d_value from fh limit 1);

