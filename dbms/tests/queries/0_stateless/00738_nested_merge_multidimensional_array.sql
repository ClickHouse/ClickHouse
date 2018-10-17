DROP TABLE IF EXISTS test.sites;
CREATE TABLE test.sites (Domain UInt8, `Users.UserID` Array(UInt64), `Users.Dates` Array(Array(Date))) ENGINE = MergeTree ORDER BY Domain SETTINGS vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0;

SYSTEM STOP MERGES;

INSERT INTO test.sites VALUES (1,[1],[[]]);
INSERT INTO test.sites VALUES (2,[1],[['2018-06-22']]);

SELECT count(), countArray(Users.Dates), countArrayArray(Users.Dates) FROM test.sites;
SYSTEM START MERGES;
OPTIMIZE TABLE test.sites FINAL;
SELECT count(), countArray(Users.Dates), countArrayArray(Users.Dates) FROM test.sites;

DROP TABLE test.sites;
