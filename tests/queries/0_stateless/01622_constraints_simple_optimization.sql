DROP TABLE IF EXISTS constraint_test_assumption;
DROP TABLE IF EXISTS constraint_test_transitivity;
DROP TABLE IF EXISTS constraint_test_transitivity2;
DROP TABLE IF EXISTS constraint_test_transitivity3;
DROP TABLE IF EXISTS constraint_test_constants_repl;
DROP TABLE IF EXISTS constraint_test_constants;

SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_substitute_columns = 1;
SET optimize_append_index = 1;

CREATE TABLE constraint_test_assumption (URL String, a Int32, CONSTRAINT c1 ASSUME domainWithoutWWW(URL) = 'bigmir.net', CONSTRAINT c2 ASSUME URL > 'zzz' AND startsWith(URL, 'test') = True) ENGINE = TinyLog;

--- Add wrong rows in order to check optimization
INSERT INTO constraint_test_assumption (URL, a) VALUES ('1', 1);
INSERT INTO constraint_test_assumption (URL, a) VALUES ('2', 2);
INSERT INTO constraint_test_assumption (URL, a) VALUES ('bigmir.net', 3);
INSERT INTO constraint_test_assumption (URL, a) VALUES ('3', 4);

SELECT count() FROM constraint_test_assumption WHERE domainWithoutWWW(URL) = 'bigmir.net'; --- assumption -> 4
SELECT count() FROM constraint_test_assumption WHERE NOT (domainWithoutWWW(URL) = 'bigmir.net'); --- assumption -> 0
SELECT count() FROM constraint_test_assumption WHERE domainWithoutWWW(URL) != 'bigmir.net'; --- assumption -> 0
SELECT count() FROM constraint_test_assumption WHERE domainWithoutWWW(URL) = 'nothing'; --- not optimized -> 0

SELECT count() FROM constraint_test_assumption WHERE (domainWithoutWWW(URL) = 'bigmir.net' AND URL > 'zzz'); ---> assumption -> 4
SELECT count() FROM constraint_test_assumption WHERE (domainWithoutWWW(URL) = 'bigmir.net' AND NOT URL <= 'zzz'); ---> assumption -> 4
SELECT count() FROM constraint_test_assumption WHERE (domainWithoutWWW(URL) = 'bigmir.net' AND URL > 'zzz') OR (a = 10 AND a + 5 < 100); ---> assumption -> 4
SELECT count() FROM constraint_test_assumption WHERE (domainWithoutWWW(URL) = 'bigmir.net' AND URL = '111'); ---> assumption & no assumption -> 0
SELECT count() FROM constraint_test_assumption WHERE (startsWith(URL, 'test') = True); ---> assumption -> 4

DROP TABLE constraint_test_assumption;

CREATE TABLE constraint_test_transitivity (a Int64, b Int64, c Int64, d Int32, CONSTRAINT c1 ASSUME a = b AND c = d, CONSTRAINT c2 ASSUME b = c) ENGINE = TinyLog;

INSERT INTO constraint_test_transitivity (a, b, c, d) VALUES (1, 2, 3, 4);

SELECT count() FROM constraint_test_transitivity WHERE a = d; ---> assumption -> 1

DROP TABLE constraint_test_transitivity;

CREATE TABLE constraint_test_strong_connectivity (a String, b String, c String, d String, CONSTRAINT c1 ASSUME a <= b AND b <= c AND c <= d AND d <= a) ENGINE = TinyLog;

INSERT INTO constraint_test_strong_connectivity (a, b, c, d) VALUES ('1', '2', '3', '4');

SELECT count() FROM constraint_test_strong_connectivity WHERE a = d; ---> assumption -> 1
SELECT count() FROM constraint_test_strong_connectivity WHERE a = c AND b = d; ---> assumption -> 1
SELECT count() FROM constraint_test_strong_connectivity WHERE a < c OR b < d; ---> assumption -> 0
SELECT count() FROM constraint_test_strong_connectivity WHERE a <= c OR b <= d; ---> assumption -> 1

DROP TABLE constraint_test_strong_connectivity;

CREATE TABLE constraint_test_transitivity2 (a String, b String, c String, d String, CONSTRAINT c1 ASSUME a > b AND b >= c AND c > d AND a >= d) ENGINE = TinyLog;

INSERT INTO constraint_test_transitivity2 (a, b, c, d) VALUES ('1', '2', '3', '4');

SELECT count() FROM constraint_test_transitivity2 WHERE a > d; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE a >= d; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE d < a; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE a < d; ---> assumption -> 0
SELECT count() FROM constraint_test_transitivity2 WHERE a = d; ---> assumption -> 0
SELECT count() FROM constraint_test_transitivity2 WHERE a != d; ---> assumption -> 1

DROP TABLE constraint_test_transitivity2;

CREATE TABLE constraint_test_transitivity3 (a Int64, b Int64, c Int64, CONSTRAINT c1 ASSUME b > 10 AND 1 > a) ENGINE = TinyLog;

INSERT INTO constraint_test_transitivity3 (a, b, c) VALUES (4, 0, 2);

SELECT count() FROM constraint_test_transitivity3 WHERE a < b; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity3 WHERE b >= a; ---> assumption -> 1

DROP TABLE constraint_test_transitivity3;

CREATE TABLE constraint_test_constants_repl (a Int64, b Int64, c Int64, d Int64, CONSTRAINT c1 ASSUME a - b = 10 AND c + d = 20) ENGINE = TinyLog;

INSERT INTO constraint_test_constants_repl (a, b, c, d) VALUES (1, 2, 3, 4);

SELECT count() FROM constraint_test_constants_repl WHERE a - b = 10; ---> assumption -> 1
SELECT count() FROM constraint_test_constants_repl WHERE a - b < 0; ---> assumption -> 0
SELECT count() FROM constraint_test_constants_repl WHERE a - b = c + d; ---> assumption -> 0
SELECT count() FROM constraint_test_constants_repl WHERE (a - b) * 2 = c + d; ---> assumption -> 1

DROP TABLE constraint_test_constants_repl;

CREATE TABLE constraint_test_constants (a Int64, b Int64, c Int64, CONSTRAINT c1 ASSUME b > 10 AND a >= 10) ENGINE = TinyLog;

INSERT INTO constraint_test_constants (a, b, c) VALUES (0, 0, 0);

SELECT count() FROM constraint_test_constants WHERE 9 < b; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 11 < b; ---> assumption -> 0
SELECT count() FROM constraint_test_constants WHERE 10 <= b; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 9 < a; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 10 < a; ---> assumption -> 0
SELECT count() FROM constraint_test_constants WHERE 10 <= a; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 9 <= a; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 11 <= a; ---> assumption -> 0

-- A AND NOT A
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100);
-- EXPLAIN QUERY TREE SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100); ---> the order of the generated checks is not consistent
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100);
EXPLAIN QUERY TREE SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) AND (c > 100);
EXPLAIN QUERY TREE SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) AND (c > 100) SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) AND (c <= 100);
EXPLAIN QUERY TREE SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) AND (c <= 100) SETTINGS enable_analyzer = 1;

DROP TABLE constraint_test_constants;
