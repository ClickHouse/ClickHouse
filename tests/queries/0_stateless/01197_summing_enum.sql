DROP TABLE IF EXISTS summing;

CREATE TABLE summing (k String, x UInt64, e Enum('hello' = 1, 'world' = 2)) ENGINE = SummingMergeTree ORDER BY k;
INSERT INTO summing SELECT '', 1, e FROM generateRandom('e Enum(\'hello\' = 1, \'world\' = 2)', 1) LIMIT 1000;
INSERT INTO summing SELECT '', 1, e FROM generateRandom('e Enum(\'hello\' = 1, \'world\' = 2)', 1) LIMIT 1000;

OPTIMIZE TABLE summing;
SELECT k, x, e FROM summing;

DROP TABLE summing;