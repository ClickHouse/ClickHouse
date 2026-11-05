DROP TABLE IF EXISTS map_containsKeyLike_test;

CREATE TABLE map_containsKeyLike_test (id UInt32, map Map(String, String)) Engine=MergeTree() ORDER BY id settings index_granularity=2;

INSERT INTO map_containsKeyLike_test VALUES (1, {'1-K1':'1-V1','1-K2':'1-V2'}),(2,{'2-K1':'2-V1','2-K2':'2-V2'});
INSERT INTO map_containsKeyLike_test VALUES (3, {'3-K1':'3-V1','3-K2':'3-V2'}),(4, {'4-K1':'4-V1','4-K2':'4-V2'});
INSERT INTO map_containsKeyLike_test VALUES (5, {'5-K1':'5-V1','5-K2':'5-V2'}),(6, {'6-K1':'6-V1','6-K2':'6-V2'});

SELECT id, map FROM map_containsKeyLike_test WHERE mapContainsKeyLike(map, '1-%') = 1;
SELECT id, map FROM map_containsKeyLike_test WHERE mapContainsKeyLike(map, '3-%') = 0 order by id;

DROP TABLE map_containsKeyLike_test;

SELECT mapContainsKeyLike(map('aa', 1, 'bb', 2), 'a%');
SELECT mapContainsKeyLike(map(toLowCardinality('aa'), 1, toLowCardinality('b'), 2), 'a%');
SELECT mapContainsKeyLike(map('aa', 1, 'bb', 2), materialize('a%'));
SELECT mapContainsKeyLike(materialize(map('aa', 1, 'bb', 2)), 'a%');
SELECT mapContainsKeyLike(materialize(map('aa', 1, 'bb', 2)), materialize('a%'));

SELECT mapContainsKeyLike(map('aa', NULL, 'bb', NULL), 'a%');
SELECT mapContainsKeyLike(map('aa', NULL, 'bb', NULL), 'q%');

SELECT mapExtractKeyLike(map('aa', NULL, 'bb', NULL), 'a%');
SELECT mapExtractKeyLike(map('aa', NULL, 'bb', NULL), 'q%');
