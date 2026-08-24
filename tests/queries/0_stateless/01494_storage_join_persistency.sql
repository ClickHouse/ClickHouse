-- Tags: no-parallel

DROP TABLE IF EXISTS join;

SELECT '----- Default Settings -----';
CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join;
SELECT * from join;

DROP TABLE join;

SELECT '----- Settings persistent=1 -----';
CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent=1;
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join;
SELECT * from join;

DROP TABLE join;

SELECT '----- Settings persistent=0 -----';
CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent=0;
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join;
SELECT * from join;

DROP TABLE join;
