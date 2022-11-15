-- Tags: no-backward-compatibility-check
DROP TABLE IF EXISTS map_test;
CREATE TABLE map_test(`tags` Map(String, String)) ENGINE = MergeTree PRIMARY KEY tags ORDER BY tags SETTINGS index_granularity = 8192;
INSERT INTO map_test (tags) VALUES (map('fruit','apple','color','red'));
INSERT INTO map_test (tags) VALUES (map('fruit','apple','color','red'));
INSERT INTO map_test (tags) VALUES (map('fruit','apple','color','red'));
INSERT INTO map_test (tags) VALUES (map('fruit','apple','color','red'));
INSERT INTO map_test (tags) VALUES (map('fruit','apple','color','red'));
SELECT mapUpdate(mapFilter((k, v) -> (k in ('fruit')), tags), map('season', 'autumn')) FROM map_test;
SELECT mapUpdate(map('season','autumn'), mapFilter((k, v) -> (k in ('fruit')), tags)) FROM map_test;
DROP TABLE map_test;
