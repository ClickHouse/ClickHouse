CREATE TEMPORARY TABLE constrained
(
    `URL` String,
    CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = domainWithoutWWW(URL),
    CONSTRAINT is_utf8 CHECK isValidUTF8(URL)
);

insert into constrained values ('a');

DROP TABLE constrained;
CREATE TABLE constrained (x UInt8, CONSTRAINT bogus CHECK 0) ENGINE = Memory();
INSERT INTO constrained VALUES (1); -- { serverError 469 }
