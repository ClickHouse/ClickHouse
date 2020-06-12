SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS mt;

CREATE TABLE mt (a Int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv AS SELECT * FROM mt;

DROP TABLE lv;
DROP TABLE mt;
