DROP TABLE IF EXISTS test_new_col;

CREATE TABLE test_new_col
(
  `_csv` String,
  `csv_as_array` Array(String) ALIAS splitByChar(';',_csv),
  `csv_col1` String DEFAULT csv_as_array[1],
  `csv_col2` String DEFAULT csv_as_array[2]
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO test_new_col (_csv) VALUES ('a1;b1;c1;d1'), ('a2;b2;c2;d2'), ('a3;b3;c3;d3');

SELECT csv_col1, csv_col2 FROM test_new_col ORDER BY csv_col1;

ALTER TABLE test_new_col ADD COLUMN `csv_col3` String DEFAULT csv_as_array[3];

SELECT csv_col3 FROM test_new_col ORDER BY csv_col3;

DROP TABLE IF EXISTS test_new_col;
