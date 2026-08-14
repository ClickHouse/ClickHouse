-- Trailing SQL comment after VALUES data (no semicolon) should be skipped, not parsed as data.
CREATE OR REPLACE TABLE test_values_trailing_comment (a String, b Int32, c Int32, d Int32) ENGINE = MergeTree() ORDER BY a;
INSERT INTO test_values_trailing_comment SETTINGS async_insert=0 VALUES
('123456', 1, 10, 100),
('123457', 2, 20, 100)
-- trailing comment after last row
;
SELECT * FROM test_values_trailing_comment ORDER BY a;
DROP TABLE test_values_trailing_comment;
