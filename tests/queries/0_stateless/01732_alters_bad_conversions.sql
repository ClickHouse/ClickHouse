DROP TABLE IF EXISTS bad_conversions;
DROP TABLE IF EXISTS bad_conversions_2;

CREATE TABLE bad_conversions (a UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO bad_conversions VALUES (1);
ALTER TABLE bad_conversions MODIFY COLUMN a Array(String); -- { serverError 53 }
SHOW CREATE TABLE bad_conversions;
SELECT count() FROM system.mutations WHERE table = 'bad_conversions' AND database = currentDatabase();

CREATE TABLE bad_conversions_2 (e Enum('foo' = 1, 'bar' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO bad_conversions_2 VALUES (1);
ALTER TABLE bad_conversions_2 MODIFY COLUMN e Enum('bar' = 1, 'foo' = 2); -- { serverError 70 }
SHOW CREATE TABLE bad_conversions_2;
SELECT count() FROM system.mutations WHERE table = 'bad_conversions_2' AND database = currentDatabase();

DROP TABLE IF EXISTS bad_conversions;
DROP TABLE IF EXISTS bad_conversions_2;
