DROP TABLE IF EXISTS test_table_comment;
CREATE VIEW test_table_comment AS SELECT toString({date_from:String});
ALTER TABLE test_table_comment MODIFY COMMENT 'test comment';
SELECT create_table_query FROM system.tables WHERE name = 'test_table_comment' AND database = currentDatabase();
DROP TABLE test_table_comment;
