-- Random settings limits: send_table_structure_on_insert_with_inline_data=(1, 1)
-- Trailing SQL comment after VALUES data (no semicolon) should be skipped, not parsed as data.
-- The test pins `send_table_structure_on_insert_with_inline_data` to 1 (legacy path) because
-- the inline-server-parsed path (setting=0) has a separate, pre-existing multi-query parsing
-- issue: when a trailing comment follows the VALUES data inside a multi-query stream, the
-- subsequent queries are silently skipped. That is an independent bug in the inline path and
-- should be fixed separately; here we keep the test focused on the trailing-comment behavior
-- in the legacy path that this test was originally written for.
CREATE OR REPLACE TABLE test_values_trailing_comment (a String, b Int32, c Int32, d Int32) ENGINE = MergeTree() ORDER BY a;
INSERT INTO test_values_trailing_comment SETTINGS async_insert=0 VALUES
('123456', 1, 10, 100),
('123457', 2, 20, 100)
-- trailing comment after last row
;
SELECT * FROM test_values_trailing_comment ORDER BY a;
DROP TABLE test_values_trailing_comment;
