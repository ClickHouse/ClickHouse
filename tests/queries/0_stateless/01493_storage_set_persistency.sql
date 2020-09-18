DROP TABLE IF EXISTS set;
DROP TABLE IF EXISTS number;

CREATE TABLE number (number UInt64) ENGINE = Memory();
INSERT INTO number values (1);

SELECT '----- Default Settings -----';
CREATE TABLE set (val UInt64) ENGINE = Set();
INSERT INTO set VALUES (1);
DETACH TABLE set;
ATTACH TABLE set (val UInt64) ENGINE = Set();
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP TABLE set;

SELECT '----- Settings persistency=1 -----';
CREATE TABLE set (val UInt64) ENGINE = Set() SETTINGS persistency=1;
INSERT INTO set VALUES (1);
DETACH TABLE set;
ATTACH TABLE set (val UInt64) ENGINE = Set() SETTINGS persistency=1;
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP TABLE set;

SELECT '----- Settings persistency=0 -----';
CREATE TABLE set (val UInt64) ENGINE = Set() SETTINGS persistency=0;
INSERT INTO set VALUES (1);
DETACH TABLE set;
ATTACH TABLE set (val UInt64) ENGINE = Set() SETTINGS persistency=0;
SELECT number FROM number WHERE number IN set LIMIT 1;

DROP TABLE set;
DROP TABLE number;