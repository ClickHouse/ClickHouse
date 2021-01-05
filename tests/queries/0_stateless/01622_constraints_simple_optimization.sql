DROP DATABASE IF EXISTS constraint_test;
DROP TABLE IF EXISTS constraint_test.constrained;

SET optimize_using_constraints = 1;

CREATE DATABASE constraint_test;
CREATE TABLE constraint_test.assumption (URL String, a Int32, CONSTRAINT c1 ASSUME domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT c2 ASSUME URL > 'zzz', CONSTRAINT c3 CHECK isValidUTF8(URL)) ENGINE = TinyLog;

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
SELECT count() FROM constraint_test.assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND URL > 'zzz') OR (a = 10 AND a + 5 < 100); ---> assumption -> 4
SELECT count() FROM constraint_test.assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND URL = '111'); ---> assumption & no assumption -> 0

DROP TABLE constraint_test.assumption;
DROP DATABASE constraint_test;