SELECT hex(SHA256(''));
SELECT hex(SHA256('abc'));

DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
    s FixedString(20)
)ENGINE = Memory();

INSERT INTO defaults SELECT s FROM generateRandom('s FixedString(20)', 1, 1, 1) LIMIT 20;

SELECT hex(SHA256(s)) FROM defaults;

DROP TABLE defaults;
