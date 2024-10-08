DROP TABLE IF EXISTS rawtable;
DROP TABLE IF EXISTS raw_to_attributes_mv;
DROP TABLE IF EXISTS attributes;

SET optimize_functions_to_subcolumns = 1;

CREATE TABLE rawtable
(
  `Attributes` Map(String, String),
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE MATERIALIZED VIEW raw_to_attributes_mv TO attributes
(
  `AttributeKeys` Array(String),
  `AttributeValues` Array(String)
)
AS SELECT
  mapKeys(Attributes) AS AttributeKeys,
  mapValues(Attributes) AS AttributeValues
FROM rawtable;

CREATE TABLE attributes
(
  `AttributeKeys` Array(String),
  `AttributeValues` Array(String)
)
ENGINE = ReplacingMergeTree
ORDER BY tuple();

INSERT INTO rawtable VALUES ({'key1': 'value1', 'key2': 'value2'});
SELECT * FROM raw_to_attributes_mv ORDER BY AttributeKeys;

DROP TABLE IF EXISTS rawtable;
DROP TABLE IF EXISTS raw_to_attributes_mv;
DROP TABLE IF EXISTS attributes;
