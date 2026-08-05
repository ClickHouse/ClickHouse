DROP TABLE IF EXISTS test_alias_inverse_order;

CREATE TABLE test_alias_inverse_order
(
  x int,
  y int ALIAS x + 1,
  z int ALIAS y + 1
)
ENGINE = MergeTree
ORDER BY ();

SELECT x, y, z FROM test_alias_inverse_order SETTINGS enable_analyzer = 1;
SELECT x, z, y FROM test_alias_inverse_order SETTINGS enable_analyzer = 1;

DROP TABLE IF EXISTS test_alias_inverse_order;
