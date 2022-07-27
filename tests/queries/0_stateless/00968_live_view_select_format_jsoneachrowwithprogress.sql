-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS lv;
DROP TABLE IF EXISTS mt;

CREATE TABLE mt (a Int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv AS SELECT * FROM mt;

INSERT INTO mt VALUES (1), (2), (3);

SELECT * FROM lv FORMAT JSONEachRowWithProgress;

DROP TABLE lv;
DROP TABLE mt;
