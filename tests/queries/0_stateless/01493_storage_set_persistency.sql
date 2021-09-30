-- Tags: no-parallel

DROP TABLE IF EXISTS set;
DROP TABLE IF EXISTS number;

CREATE TABLE number (number UInt64) ENGINE = Memory();
INSERT INTO number values (1);

SELECT '----- Default Settings -----';
CREATE TABLE set (val UInt64) ENGINE = Set();
INSERT INTO set VALUES (1);
DETACH TABLE set;
ATTACH TABLE set;
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP TABLE set;

SELECT '----- Settings persistent=1 -----';
CREATE TABLE set (val UInt64) ENGINE = Set() SETTINGS persistent=1;
INSERT INTO set VALUES (1);
DETACH TABLE set;
ATTACH TABLE set;
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP TABLE set;

SELECT '----- Settings persistent=0 -----';
CREATE TABLE set (val UInt64) ENGINE = Set() SETTINGS persistent=0;
INSERT INTO set VALUES (1);
DETACH TABLE set;
ATTACH TABLE set;
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP TABLE set;
DROP TABLE number;
