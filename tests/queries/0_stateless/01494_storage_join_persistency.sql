DROP TABLE IF EXISTS join;

SELECT '----- Default Settings -----';
CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k);
SELECT * from join;

DROP TABLE join;

SELECT '----- Settings persistency=1 -----';
CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistency=1;
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistency=1;
SELECT * from join;

DROP TABLE join;

SELECT '----- Settings persistency=0 -----';
CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistency=0;
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistency=0;
SELECT * from join;

DROP TABLE join;
