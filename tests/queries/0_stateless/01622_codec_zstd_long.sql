DROP TABLE IF EXISTS zstd_1_00;
DROP TABLE IF EXISTS zstd_1_24;
DROP TABLE IF EXISTS zstd_9_00;
DROP TABLE IF EXISTS zstd_9_24;
DROP TABLE IF EXISTS words;

CREATE TABLE words(i Int, word String) ENGINE = Memory;
INSERT INTO words SELECT * FROM generateRandom('i Int, word String',1,10) LIMIT 10000;

CREATE TABLE zstd_1_00(n Int, b String CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY n;
CREATE TABLE zstd_1_24(n Int, b String CODEC(ZSTD(1,24))) ENGINE = MergeTree ORDER BY n;
CREATE TABLE zstd_9_00(n Int, b String CODEC(ZSTD(9))) ENGINE = MergeTree ORDER BY n;
CREATE TABLE zstd_9_24(n Int, b String CODEC(ZSTD(9,24))) ENGINE = MergeTree ORDER BY n;

INSERT INTO zstd_1_00 SELECT * FROM words;
INSERT INTO zstd_1_24 SELECT * FROM words;
INSERT INTO zstd_9_00 SELECT * FROM words;
INSERT INTO zstd_9_24 SELECT * FROM words;

SELECT COUNT(n) FROM zstd_1_00 LEFT JOIN words ON i == n WHERE b == word;
SELECT COUNT(n) FROM zstd_1_24 LEFT JOIN words ON i == n WHERE b == word;
SELECT COUNT(n) FROM zstd_9_00 LEFT JOIN words ON i == n WHERE b == word;
SELECT COUNT(n) FROM zstd_9_24 LEFT JOIN words ON i == n WHERE b == word;

DROP TABLE zstd_1_00;
DROP TABLE zstd_1_24;
DROP TABLE zstd_9_00;
DROP TABLE zstd_9_24;
DROP TABLE words;
