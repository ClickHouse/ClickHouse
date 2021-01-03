DROP DATABASE IF EXISTS constraint_test;
DROP TABLE IF EXISTS constraint_test.constrained;

SET optimize_using_constraints = 1;

CREATE DATABASE constraint_test;
CREATE TABLE constraint_test.assumption (URL String, CONSTRAINT is_yandex ASSUME domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = TinyLog;

--- Add wrong rows in order to check optimization
INSERT INTO constraint_test.assumption (URL) VALUES ('1');
INSERT INTO constraint_test.assumption (URL) VALUES ('2');
INSERT INTO constraint_test.assumption (URL) VALUES ('yandex.ru');
INSERT INTO constraint_test.assumption (URL) VALUES ('3');

SELECT count() FROM constraint_test.assumption WHERE domainWithoutWWW(URL) = 'yandex.ru'; --- assumption -> 4
SELECT count() FROM constraint_test.assumption WHERE NOT (domainWithoutWWW(URL) = 'yandex.ru'); --- assumption -> 0
SELECT count() FROM constraint_test.assumption WHERE domainWithoutWWW(URL) != 'yandex.ru'; --- not optimized -> 3
SELECT count() FROM constraint_test.assumption WHERE domainWithoutWWW(URL) = 'nothing'; --- not optimized -> 0

DROP TABLE constraint_test.assumption;
DROP DATABASE constraint_test;