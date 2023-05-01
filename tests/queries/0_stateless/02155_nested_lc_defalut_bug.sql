DROP TABLE IF EXISTS nested_test;
CREATE TABLE nested_test (x UInt32, `nest.col1` Array(String), `nest.col2` Array(Int8)) ENGINE = MergeTree ORDER BY x;

ALTER TABLE nested_test ADD COLUMN `nest.col3` Array(LowCardinality(String));
INSERT INTO nested_test (x, `nest.col1`, `nest.col2`) values (1, ['a', 'b'], [3, 4]);
SELECT * FROM nested_test;

DROP TABLE IF EXISTS nested_test;
