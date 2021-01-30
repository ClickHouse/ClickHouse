DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
	n Int32,
	s String
)ENGINE = Memory();

INSERT INTO defaults VALUES(1, '1') (2, '2') (3, '3') (4, '4') (5, '5');

SELECT * FROM defaults;

ALTER TABLE defaults UPDATE n = 100 WHERE s = '1';

SELECT * FROM defaults;

ALTER TABLE defaults DELETE WHERE n = 100;

SELECT * FROM defaults;

DROP TABLE defaults;
