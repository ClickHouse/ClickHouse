DROP TABLE IF EXISTS table_views_test;
CREATE TABLE table_views_test (a UInt8, s String) ENGINE = MergeTree() ORDER BY a;
DROP TABLE IF EXISTS table_views_test_view;
CREATE MATERIALIZED VIEW table_views_test_view ENGINE = ReplacingMergeTree() ORDER BY a AS SELECT * FROM table_views_test;
SELECT * FROM system.table_views WHERE view_table = 'table_views_test_view';
DROP TABLE IF EXISTS table_views_test_view;
SELECT * FROM system.table_views WHERE view_table = 'table_views_test_view';
DROP TABLE IF EXISTS table_views_test;
