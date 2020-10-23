DROP TABLE IF EXISTS test.check_comments;

CREATE TABLE test.check_comments
  (
    column_name1 UInt8 DEFAULT 1 COMMENT 'comment',
    column_name2 UInt8 COMMENT 'non default comment'
  ) ENGINE = ReplicatedMergeTree('clickhouse/tables/test_comments', 'r1')
    ORDER BY column_name1;

SHOW CREATE test.check_comments;
DESC test.check_comments;

ALTER TABLE test.check_comments COMMENT COLUMN column_name1 'another comment';

SHOW CREATE test.check_comments;
DESC test.check_comments;

SELECT * FROM system.columns WHERE table = 'check.comments' and database = 'test';

DROP TABLE test.check_comments;
