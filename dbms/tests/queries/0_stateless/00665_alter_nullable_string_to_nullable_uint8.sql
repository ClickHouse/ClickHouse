DROP TABLE IF EXISTS alter;
CREATE TABLE alter (`boolean_false` Nullable(String)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO alter (`boolean_false`) VALUES (NULL), (''), ('123');
SELECT * FROM alter;
SELECT * FROM alter ORDER BY boolean_false NULLS LAST;

ALTER TABLE alter MODIFY COLUMN `boolean_false` Nullable(UInt8);
SELECT * FROM alter;
SELECT * FROM alter ORDER BY boolean_false NULLS LAST;

DROP TABLE alter;
