DROP TABLE IF EXISTS map_containsValueLike_test;

CREATE TABLE map_containsValueLike_test (id UInt32, map Map(String, String)) Engine=MergeTree() ORDER BY id settings index_granularity=2;

INSERT INTO map_containsValueLike_test VALUES (1, {'1-K1':'1-V1','1-K2':'1-V2'}),(2,{'2-K1':'2-V1','2-K2':'2-V2'});
INSERT INTO map_containsValueLike_test VALUES (3, {'3-K1':'3-V1','3-K2':'3-V2'}),(4, {'4-K1':'4-V1','4-K2':'4-V2'});
INSERT INTO map_containsValueLike_test VALUES (5, {'5-K1':'5-V1','5-K2':'5-V2'}),(6, {'6-K1':'6-V1','6-K2':'6-V2'});

SELECT id, map FROM map_containsValueLike_test WHERE mapContainsValueLike(map, '1-%') = 1;
SELECT id, map FROM map_containsValueLike_test WHERE mapContainsValueLike(map, '3-%') = 0 order by id;

DROP TABLE map_containsValueLike_test;

SELECT mapContainsValueLike(map('aa', '1', 'bb', '2'), '1%');
SELECT mapContainsValueLike(map('aa', toLowCardinality('1'), 'b', toLowCardinality('2')), '1%');
SELECT mapContainsValueLike(map('aa', '1', 'bb', '2'), materialize('1%'));
SELECT mapContainsValueLike(materialize(map('aa', '1', 'bb', '2')), '1%');
SELECT mapContainsValueLike(materialize(map('aa', '1', 'bb', '2')), materialize('1%'));

SELECT mapContainsValueLike(map('aa', 'cc', 'bb', 'dd'), 'd%');
SELECT mapContainsValueLike(map('aa', 'cc', 'bb', 'dd'), 'q%');

SELECT mapExtractValueLike(map('aa', 'cc', 'bb', 'dd'), 'd%');
SELECT mapExtractValueLike(map('aa', 'cc', 'bb', 'dd'), 'q%');
