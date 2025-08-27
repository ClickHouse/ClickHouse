-- Disable this setting to properly test String type
SET data_type_string_use_size_stream = 0;

DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
	n Int32,
	s String
)ENGINE = Memory();

ALTER TABLE defaults ADD COLUMN m Int8;
ALTER TABLE defaults DROP COLUMN n;

DESC TABLE defaults;

DROP TABLE defaults;
