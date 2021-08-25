DROP TABLE IF EXISTS views_test;
CREATE TABLE views_test (a UInt8, s String) ENGINE = MergeTree() ORDER BY a;
DROP TABLE IF EXISTS views_test_view;
CREATE MATERIALIZED VIEW views_test_view ENGINE = ReplacingMergeTree() ORDER BY a AS SELECT * FROM views_test;
SELECT * FROM system.views WHERE view_table = 'views_test_view';
DROP TABLE IF EXISTS views_test_view;
SELECT * FROM system.views WHERE view_table = 'views_test_view';
DROP TABLE IF EXISTS views_test;
