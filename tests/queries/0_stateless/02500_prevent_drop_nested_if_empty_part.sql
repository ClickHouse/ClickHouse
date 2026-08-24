DROP TABLE IF EXISTS 02500_nested;

SET flatten_nested = 1;

CREATE TABLE 02500_nested(nes Nested(a Int32, b Int32)) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(nes.a, nes.b) VALUES ([1], [2]);
ALTER TABLE 02500_nested ADD COLUMN z Int32;
ALTER TABLE 02500_nested DROP COLUMN nes; -- { serverError BAD_ARGUMENTS }
DROP TABLE 02500_nested;

CREATE TABLE 02500_nested(nes Nested(a Int32, b Int32), z Int32) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(nes.a, nes.b, z) VALUES ([1], [2], 2);
ALTER TABLE 02500_nested DROP COLUMN nes;
DROP TABLE 02500_nested;

SET flatten_nested = 0;

CREATE TABLE 02500_nested(nes Nested(a Int32, b Int32)) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(nes) VALUES ([(1, 2)]);
ALTER TABLE 02500_nested ADD COLUMN z Int32;
ALTER TABLE 02500_nested DROP COLUMN nes; -- { serverError BAD_ARGUMENTS }
DROP TABLE 02500_nested;

CREATE TABLE 02500_nested(nes Array(Tuple(a Int32, b Int32)), z Int32) Engine=MergeTree ORDER BY tuple();
INSERT INTO 02500_nested(nes, z) VALUES ([(1, 2)], 2);
ALTER TABLE 02500_nested DROP COLUMN nes;
DROP TABLE 02500_nested;
