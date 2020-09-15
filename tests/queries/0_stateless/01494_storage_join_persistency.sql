DROP TABLE IF EXISTS join;

CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS disable_persistency=1;

DETACH TABLE join;
ATTACH TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS disable_persistency=1;

DROP TABLE join;
