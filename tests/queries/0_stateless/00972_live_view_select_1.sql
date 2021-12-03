SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS lv;

CREATE LIVE VIEW lv AS SELECT 1;

SELECT * FROM lv;

DROP TABLE lv;
