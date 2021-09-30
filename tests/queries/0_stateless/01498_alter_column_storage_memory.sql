DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
	n Int32,
	s String
)ENGINE = Memory();

ALTER TABLE defaults ADD COLUMN m Int8; -- { serverError 48 }
ALTER TABLE defaults DROP COLUMN n; -- { serverError 48 }

DROP TABLE defaults;
