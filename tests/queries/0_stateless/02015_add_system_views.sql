DROP TABLE IF EXISTS default.views_test;
CREATE TABLE default.views_test (a UInt8, s String) ENGINE = MergeTree() ORDER BY a;
DROP TABLE IF EXISTS default.views_test_view;
CREATE MATERIALIZED VIEW default.views_test_view ENGINE = ReplacingMergeTree() ORDER BY a AS SELECT * FROM default.views_test;
SELECT * FROM system.views WHERE database = 'default' and name = 'views_test_view';
DROP TABLE IF EXISTS default.views_test_view;
SELECT * FROM system.views WHERE database = 'default' and name = 'views_test_view';
DROP TABLE IF EXISTS default.views_test;
