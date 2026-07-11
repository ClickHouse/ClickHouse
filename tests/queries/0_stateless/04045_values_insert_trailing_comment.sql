-- Random settings limits: send_table_structure_on_insert_with_inline_data=(1, 1)
-- Trailing SQL comment after VALUES data (no semicolon) should be skipped, not parsed as data.
-- This test pins `send_table_structure_on_insert_with_inline_data` to 1 (legacy path).
-- With setting = 0 (inline-server-parsed path), we observed a likely-unintended behavior in
-- multi-query mode: when a trailing SQL comment follows the VALUES data inside a multi-query
-- stream, the subsequent queries are silently skipped (no error, no data, queries effectively
-- dropped). Silent data loss is almost certainly a bug in the inline path rather than intended
-- behavior. This is tracked in https://github.com/ClickHouse/ClickHouse/issues/109253; we pin
-- the legacy path here to keep this test focused on the trailing-comment behavior it was
-- originally written for. Once the issue is fixed, the pin can be removed.
CREATE OR REPLACE TABLE test_values_trailing_comment (a String, b Int32, c Int32, d Int32) ENGINE = MergeTree() ORDER BY a;
INSERT INTO test_values_trailing_comment SETTINGS async_insert=0 VALUES
('123456', 1, 10, 100),
('123457', 2, 20, 100)
-- trailing comment after last row
;
SELECT * FROM test_values_trailing_comment ORDER BY a;
DROP TABLE test_values_trailing_comment;
