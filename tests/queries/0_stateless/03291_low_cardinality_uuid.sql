-- creating this data type is allowed by default:
CREATE TEMPORARY TABLE test (x LowCardinality(UUID));

-- you can use it:
INSERT INTO test VALUES ('e1b005c0-b947-4893-be97-c9390d0aa583');
SELECT * FROM test;
