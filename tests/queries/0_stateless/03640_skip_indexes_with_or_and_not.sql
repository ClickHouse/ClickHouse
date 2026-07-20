-- Testcase for https://github.com/ClickHouse/ClickHouse/issues/94020
-- Test for skip indexes with OR and NOT in the WHERE clause

SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
  id UInt32,
  map1 Map(String, String),
  map2 Map(String, String),
  INDEX idx_map1_key mapKeys(map1) TYPE bloom_filter(0.01) GRANULARITY 1,
  INDEX idx_map2_key mapKeys(map2) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PRIMARY KEY(id);

INSERT INTO tab VALUES
  (1, {'key1':'1'}, {'key1':'1','key2':'2'}),
  (2, {'key1':'1','key2':'2'}, {'key1':'1'}),
  (3, {'key1':'1','key2':'2','key3':'3'}, {}),
  (4, {'key1':'1','key2':'2'}, {'key1':'1'}),
  (5, {'key1':'1'}, {'key3':'3'}),
  (6, {'key1':'1'}, {'key1':'1'});

SELECT *
FROM tab
WHERE (mapContains(map1, 'key2') OR mapContains(map2, 'key2')) AND (NOT mapContains(map2, 'key3'))
ORDER BY id;
