DROP TABLE IF EXISTS override_test;
CREATE TABLE override_test (_part UInt32) ENGINE = MergeTree ORDER BY tuple() AS SELECT 1;
SELECT _part FROM override_test;
