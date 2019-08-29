SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS test.lv;
DROP TABLE IF EXISTS test.mt;

CREATE TABLE test.mt (a Int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW test.lv AS SELECT sum(a) FROM test.mt;

WATCH test.lv LIMIT 0 FORMAT JSONEachRowWithProgress;

INSERT INTO test.mt VALUES (1),(2),(3);

WATCH test.lv LIMIT 0 FORMAT JSONEachRowWithProgress;

INSERT INTO test.mt VALUES (4),(5),(6);

WATCH test.lv LIMIT 0 FORMAT JSONEachRowWithProgress;

DROP TABLE test.lv;
DROP TABLE test.mt;
