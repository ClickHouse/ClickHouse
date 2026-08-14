DROP TABLE IF EXISTS map_extractKeyLike_test;

CREATE TABLE map_extractKeyLike_test (id UInt32, map Map(String, String)) Engine=MergeTree() ORDER BY id settings index_granularity=2;

INSERT INTO map_extractKeyLike_test VALUES (1, {'P1-K1':'1-V1','P2-K2':'1-V2'}),(2,{'P1-K1':'2-V1','P2-K2':'2-V2'});
INSERT INTO map_extractKeyLike_test VALUES (3, {'P1-K1':'3-V1','P2-K2':'3-V2'}),(4,{'P1-K1':'4-V1','P2-K2':'4-V2'});
INSERT INTO map_extractKeyLike_test VALUES (5, {'5-K1':'5-V1','5-K2':'5-V2'}),(6, {'P3-K1':'6-V1','P4-K2':'6-V2'});

SELECT 'The data of table:';
SELECT * FROM map_extractKeyLike_test ORDER BY id;

SELECT '';

SELECT 'The results of query: SELECT id, mapExtractKeyLike(map, \'P1%\') FROM map_extractKeyLike_test ORDER BY id;';
SELECT id, mapExtractKeyLike(map, 'P1%') FROM map_extractKeyLike_test ORDER BY id;

SELECT '';

SELECT 'The results of query: SELECT id, mapExtractKeyLike(map, \'5-K1\') FROM map_extractKeyLike_test ORDER BY id;';
SELECT id, mapExtractKeyLike(map, '5-K1') FROM map_extractKeyLike_test ORDER BY id;

DROP TABLE map_extractKeyLike_test;

SELECT mapExtractKeyLike(map('aa', 1, 'bb', 2), 'a%');
SELECT mapExtractKeyLike(map('aa', 1, 'bb', 2), materialize('a%'));
SELECT mapExtractKeyLike(materialize(map('aa', 1, 'bb', 2)), 'a%');
SELECT mapExtractKeyLike(materialize(map('aa', 1, 'bb', 2)), materialize('a%'));
