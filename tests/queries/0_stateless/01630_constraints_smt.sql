DROP TABLE IF EXISTS constraint_test_assumption;
DROP TABLE IF EXISTS constraint_test_transitivity;
DROP TABLE IF EXISTS constraint_test_transitivity2;

SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_using_smt = 1;
--SET optimize_substitute_columns = 1;
--SET optimize_append_index = 1;


SELECT 'TEST1';
CREATE TABLE constraint_test_transitivity (a Int64, b Int64, c Int64, d Int32, CONSTRAINT c1 ASSUME a = b AND c = d, CONSTRAINT c2 ASSUME b = c) ENGINE = TinyLog;

INSERT INTO constraint_test_transitivity (a, b, c, d) VALUES (1, 2, 3, 4);

SELECT count() FROM constraint_test_transitivity WHERE a = d; ---> assumption -> 1

DROP TABLE constraint_test_transitivity;

SELECT 'TEST2';
CREATE TABLE constraint_test_strong_connectivity (a Float64, b Float64, c UInt32, d Float64, CONSTRAINT c1 ASSUME a <= b AND b <= c AND c <= d AND d <= a) ENGINE = TinyLog;

INSERT INTO constraint_test_strong_connectivity (a, b, c, d) VALUES (1.1, 2.2, 3, 4.0);

SELECT count() FROM constraint_test_strong_connectivity WHERE a = d; ---> assumption -> 1

--fsda;jfwioejfriaohwneafo
SELECT count() FROM constraint_test_strong_connectivity WHERE a = c AND b = d; ---> assumption -> 1
SELECT count() FROM constraint_test_strong_connectivity WHERE a < c OR b < d; ---> assumption -> 0
SELECT count() FROM constraint_test_strong_connectivity WHERE a <= c OR b <= d; ---> assumption -> 1

DROP TABLE constraint_test_strong_connectivity;

SELECT 'TEST3';
CREATE TABLE constraint_test_transitivity2 (a Float64, b Float64, c Float64, d Float64, CONSTRAINT c1 ASSUME a > b AND b >= c AND c > d AND a >= d) ENGINE = TinyLog;

INSERT INTO constraint_test_transitivity2 (a, b, c, d) VALUES (1.0, 2.2, 3.3, 4.0);

SELECT count() FROM constraint_test_transitivity2 WHERE a > d; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE a >= d; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE d < a; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE a < d; ---> assumption -> 0
SELECT count() FROM constraint_test_transitivity2 WHERE a = d; ---> assumption -> 0
SELECT count() FROM constraint_test_transitivity2 WHERE a != d; ---> assumption -> 1

DROP TABLE constraint_test_transitivity2;

SELECT 'TEST4';
CREATE TABLE constraint_test_transitivity3 (a Int64, b Int64, c Int64, s String, CONSTRAINT c1 ASSUME b > 10 AND 1 > a AND s = 'abs', CONSTRAINT c2 ASSUME upperUTF8(s) = format(s, 'CLICKHOUSE')) ENGINE = TinyLog;

INSERT INTO constraint_test_transitivity3 (a, b, c, s) VALUES (4, 0, 2, '{}!');

SELECT count() FROM constraint_test_transitivity3 WHERE a < b; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity3 WHERE a < b AND upperUTF8(s) = format(s, 'CLICKHOUSE'); ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity3 WHERE a < b OR upperUTF8(s) = format(s, 'CLICKHOUSE'); ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity3 WHERE a > b AND upperUTF8(s) = format(s, 'CLICKHOUSE'); ---> assumption -> 0
SELECT count() FROM constraint_test_transitivity3 WHERE a > b OR upperUTF8(s) = format(s, 'CLICKHOUSE'); ---> assumption -> 0
SELECT count() FROM constraint_test_transitivity3 WHERE b >= a; ---> assumption -> 1

DROP TABLE constraint_test_transitivity3;

SELECT 'TEST5';
CREATE TABLE constraint_test_constants_repl (a Int64, b Int64, c Int64, d Int64, CONSTRAINT c1 ASSUME a - b = 10 AND c + d = 20) ENGINE = TinyLog;

INSERT INTO constraint_test_constants_repl (a, b, c, d) VALUES (1, 2, 3, 4);

SELECT count() FROM constraint_test_constants_repl WHERE a - b = 10; ---> assumption -> 1
SELECT count() FROM constraint_test_constants_repl WHERE a - b < 0; ---> assumption -> 0
SELECT count() FROM constraint_test_constants_repl WHERE a - b = c + d; ---> assumption -> 0
SELECT count() FROM constraint_test_constants_repl WHERE (a - b) * 2 = c + d; ---> assumption -> 1

DROP TABLE constraint_test_constants_repl;

SELECT 'TEST6';
CREATE TABLE constraint_test_constants (a Int64, b Int64, c Int64, CONSTRAINT c1 ASSUME b > 10 AND a >= 10) ENGINE = TinyLog;

INSERT INTO constraint_test_constants (a, b, c) VALUES (0, 0, 0);

SELECT count() FROM constraint_test_constants WHERE 9 < b; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 11 < b; ---> assumption -> 0
SELECT count() FROM constraint_test_constants WHERE 10 <= b; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 9 < a; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 10 < a; ---> assumption -> 0
SELECT count() FROM constraint_test_constants WHERE 9 <= a; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 11 <= a; ---> assumption -> 0

-- A AND NOT A
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100);
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100);
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) AND (c > 100);
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) AND (c <= 100);

DROP TABLE constraint_test_constants;

SELECT 'TEST7';

CREATE TABLE constraint_test_constants_fail (a String, b String, CONSTRAINT c1 ASSUME a > reverse(b)) ENGINE = TinyLog;

EXPLAIN SYNTAX SELECT a, b FROM constraint_test_constants_fail WHERE a = 'c';
EXPLAIN SYNTAX SELECT a, b FROM constraint_test_constants_fail WHERE a < reverse(b);

DROP TABLE constraint_test_constants_fail;
