SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS lv;
DROP TABLE IF EXISTS mt;

CREATE TABLE mt (a Int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv AS SELECT sum(a) FROM mt;

WATCH lv EVENTS LIMIT 0;

INSERT INTO mt VALUES (1),(2),(3);

WATCH lv EVENTS LIMIT 0;

INSERT INTO mt VALUES (4),(5),(6);

WATCH lv EVENTS LIMIT 0;

DROP TABLE lv;
DROP TABLE mt;
