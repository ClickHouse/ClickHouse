DROP DATABASE IF EXISTS constraint_test;
DROP TABLE IF EXISTS constraint_test.assumption;
DROP TABLE IF EXISTS constraint_test.transitivity;
DROP TABLE IF EXISTS constraint_test.transitivity2;

SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;

CREATE DATABASE constraint_test;
CREATE TABLE constraint_test.assumption (URL String, a Int32, CONSTRAINT c1 ASSUME domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT c2 ASSUME URL > 'zzz' AND startsWith(URL, 'test') = True) ENGINE = TinyLog;

--- Add wrong rows in order to check optimization
INSERT INTO constraint_test.assumption (URL, a) VALUES ('1', 1);
INSERT INTO constraint_test.assumption (URL, a) VALUES ('2', 2);
INSERT INTO constraint_test.assumption (URL, a) VALUES ('yandex.ru', 3);
INSERT INTO constraint_test.assumption (URL, a) VALUES ('3', 4);

SELECT count() FROM constraint_test.assumption WHERE domainWithoutWWW(URL) = 'yandex.ru'; --- assumption -> 4
SELECT count() FROM constraint_test.assumption WHERE NOT (domainWithoutWWW(URL) = 'yandex.ru'); --- assumption -> 0
SELECT count() FROM constraint_test.assumption WHERE domainWithoutWWW(URL) != 'yandex.ru'; --- assumption -> 0
SELECT count() FROM constraint_test.assumption WHERE domainWithoutWWW(URL) = 'nothing'; --- not optimized -> 0

SELECT count() FROM constraint_test.assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND URL > 'zzz'); ---> assumption -> 4
SELECT count() FROM constraint_test.assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND NOT URL <= 'zzz'); ---> assumption -> 4
SELECT count() FROM constraint_test.assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND URL > 'zzz') OR (a = 10 AND a + 5 < 100); ---> assumption -> 4
SELECT count() FROM constraint_test.assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND URL = '111'); ---> assumption & no assumption -> 0
SELECT count() FROM constraint_test.assumption WHERE (startsWith(URL, 'test') = True); ---> assumption -> 4

DROP TABLE constraint_test.assumption;

CREATE TABLE constraint_test.transitivity (a Int64, b Int64, c Int64, d Int32, CONSTRAINT c1 ASSUME a = b AND c = d, CONSTRAINT c2 ASSUME b = c) ENGINE = TinyLog;

INSERT INTO constraint_test.transitivity (a, b, c, d) VALUES (1, 2, 3, 4);

SELECT count() FROM constraint_test.transitivity WHERE a = d; ---> assumption -> 1

DROP TABLE constraint_test.transitivity;


CREATE TABLE constraint_test.strong_connectivity (a String, b String, c String, d String, CONSTRAINT c1 ASSUME a <= b AND b <= c AND c <= d AND d <= a) ENGINE = TinyLog;

INSERT INTO constraint_test.strong_connectivity (a, b, c, d) VALUES ('1', '2', '3', '4');

SELECT count() FROM constraint_test.strong_connectivity WHERE a = d; ---> assumption -> 1
SELECT count() FROM constraint_test.strong_connectivity WHERE a = c AND b = d; ---> assumption -> 1
SELECT count() FROM constraint_test.strong_connectivity WHERE a < c OR b < d; ---> assumption -> 0
SELECT count() FROM constraint_test.strong_connectivity WHERE a <= c OR b <= d; ---> assumption -> 1

DROP TABLE constraint_test.strong_connectivity;

CREATE TABLE constraint_test.transitivity2 (a String, b String, c String, d String, CONSTRAINT c1 ASSUME a > b AND b >= c AND c > d AND a >= d) ENGINE = TinyLog;

INSERT INTO constraint_test.transitivity2 (a, b, c, d) VALUES ('1', '2', '3', '4');

SELECT count() FROM constraint_test.transitivity2 WHERE a > d; ---> assumption -> 1
SELECT count() FROM constraint_test.transitivity2 WHERE a >= d; ---> assumption -> 1
SELECT count() FROM constraint_test.transitivity2 WHERE d < a; ---> assumption -> 1
SELECT count() FROM constraint_test.transitivity2 WHERE a < d; ---> assumption -> 0
SELECT count() FROM constraint_test.transitivity2 WHERE a = d; ---> assumption -> 0

DROP TABLE constraint_test.transitivity2;


CREATE TABLE constraint_test.constants_repl (a Int64, b Int64, c Int64, d Int64, CONSTRAINT c1 ASSUME a - b = 10 AND c + d = 20) ENGINE = TinyLog;

INSERT INTO constraint_test.constants_repl (a, b, c, d) VALUES (1, 2, 3, 4);

SELECT count() FROM constraint_test.constants_repl WHERE a - b = 10; ---> assumption -> 1
SELECT count() FROM constraint_test.constants_repl WHERE a - b < 0; ---> assumption -> 0
SELECT count() FROM constraint_test.constants_repl WHERE a - b = c + d; ---> assumption -> 0
SELECT count() FROM constraint_test.constants_repl WHERE (a - b) * 2 = c + d; ---> assumption -> 1

DROP TABLE constraint_test.constants_repl;

DROP DATABASE constraint_test;