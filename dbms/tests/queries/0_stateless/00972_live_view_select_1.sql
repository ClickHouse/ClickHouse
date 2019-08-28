SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS test.lv;

CREATE LIVE VIEW test.lv AS SELECT 1;

SELECT * FROM test.lv;

DROP TABLE test.lv;
