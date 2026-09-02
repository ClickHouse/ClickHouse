DROP TABLE IF EXISTS summing_merge_tree_datetime64;

CREATE TABLE summing_merge_tree_datetime64 ( `pk` UInt64, `timestamp` DateTime64(3), `value` UInt64 )
ENGINE = SummingMergeTree() ORDER BY pk;

INSERT INTO summing_merge_tree_datetime64 SELECT 1 pk, '2023-05-01 23:55:55.100' timestamp, 1 value;
INSERT INTO summing_merge_tree_datetime64 SELECT 1 pk, '2023-05-01 23:55:55.100' timestamp, 2 value;
INSERT INTO summing_merge_tree_datetime64 SELECT 1 pk, '2023-05-01 23:55:55.100' timestamp, 3 value;
INSERT INTO summing_merge_tree_datetime64 SELECT 1 pk, '2023-05-01 23:55:55.100' timestamp, 4 value;
INSERT INTO summing_merge_tree_datetime64 SELECT 1 pk, '2023-05-01 23:55:55.100' timestamp, 5 value;

SELECT * FROM summing_merge_tree_datetime64 FINAL;
DROP TABLE summing_merge_tree_datetime64;
