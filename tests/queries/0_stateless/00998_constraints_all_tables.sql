DROP TABLE IF EXISTS constrained;
CREATE TABLE constrained (URL String, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = Null;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
DROP TABLE constrained;

CREATE TABLE constrained (URL String, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = Memory;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
SELECT count() FROM constrained;
DROP TABLE constrained;

CREATE TABLE constrained (URL String, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = StripeLog;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
SELECT count() FROM constrained;
DROP TABLE constrained;

CREATE TABLE constrained (URL String, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = TinyLog;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
SELECT count() FROM constrained;
DROP TABLE constrained;

CREATE TABLE constrained (URL String, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = Log;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
SELECT count() FROM constrained;
DROP TABLE constrained;


DROP TABLE IF EXISTS constrained2;
CREATE TABLE constrained (URL String, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = Log;
CREATE TABLE constrained2 AS constrained;
SHOW CREATE TABLE constrained;
SHOW CREATE TABLE constrained2;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
INSERT INTO constrained2 VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
DROP TABLE constrained;
DROP TABLE constrained2;
