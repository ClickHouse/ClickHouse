DROP TABLE IF EXISTS set;

CREATE TABLE set (x String) ENGINE = Set() SETTINGS disable_persistency=1;

DETACH TABLE set;
ATTACH TABLE set (x String) ENGINE = Set() SETTINGS disable_persistency=1;

DROP TABLE set;