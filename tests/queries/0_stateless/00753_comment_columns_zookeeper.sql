DROP TABLE IF EXISTS check_comments;

CREATE TABLE check_comments
  (
    column_name1 UInt8 DEFAULT 1 COMMENT 'comment',
    column_name2 UInt8 COMMENT 'non default comment'
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_00753/comments', 'r1')
    ORDER BY column_name1;

SHOW CREATE check_comments;
DESC check_comments;

ALTER TABLE check_comments COMMENT COLUMN column_name1 'another comment';

SHOW CREATE check_comments;
DESC check_comments;

SELECT * FROM system.columns WHERE table = 'check.comments' and database = currentDatabase();

DROP TABLE check_comments;
