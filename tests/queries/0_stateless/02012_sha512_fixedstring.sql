-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

SELECT hex(SHA512(''));
SELECT hex(SHA512('abc'));

SELECT hex(SHA512_256(''));
SELECT hex(SHA512_256('abc'));

DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
    s FixedString(20)
)ENGINE = Memory();

INSERT INTO defaults SELECT s FROM generateRandom('s FixedString(20)', 1, 1, 1) LIMIT 20;

SELECT hex(SHA512(s)) FROM defaults;
SELECT hex(SHA512_256(s)) FROM defaults;

DROP TABLE defaults;
