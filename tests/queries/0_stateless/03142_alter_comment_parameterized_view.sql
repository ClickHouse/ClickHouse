DROP TABLE IF EXISTS test_table_comment;
CREATE VIEW test_table_comment AS SELECT toString({date_from:String});
ALTER TABLE test_table_comment MODIFY COMMENT 'test comment';
DROP TABLE test_table_comment;
