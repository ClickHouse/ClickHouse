CREATE TEMPORARY TABLE test (name String, event Int8) ENGINE = MergeTree ORDER BY (name) SETTINGS allow_experimental_optimized_row_order = True;
INSERT INTO test VALUES ('Igor', 3), ('Egor', 1), ('Egor', 2), ('Igor', 2), ('Igor', 1);
SELECT * FROM test ORDER BY (name) SETTINGS optimize_read_in_order=1;
