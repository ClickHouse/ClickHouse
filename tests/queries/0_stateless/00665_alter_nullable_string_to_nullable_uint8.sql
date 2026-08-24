DROP TABLE IF EXISTS alter_00665;
CREATE TABLE alter_00665 (`boolean_false` Nullable(String)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO alter_00665 (`boolean_false`) VALUES (NULL), (''), ('123');
SELECT * FROM alter_00665;
SELECT * FROM alter_00665 ORDER BY boolean_false NULLS LAST;

ALTER TABLE alter_00665 MODIFY COLUMN `boolean_false` Nullable(UInt8);
SELECT * FROM alter_00665;
SELECT * FROM alter_00665 ORDER BY boolean_false NULLS LAST;

DROP TABLE alter_00665;
