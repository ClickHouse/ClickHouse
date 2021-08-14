DROP TABLE IF EXISTS check_query_comment_column;

-- Check COMMENT COLUMN and MODIFY COLUMN statements with simple engine
CREATE TABLE check_query_comment_column
  (
    first_column UInt8 DEFAULT 1 COMMENT 'comment 1',
    second_column UInt8 MATERIALIZED first_column COMMENT 'comment 2',
    third_column UInt8 ALIAS second_column COMMENT 'comment 3',
    fourth_column UInt8 COMMENT 'comment 4',
    fifth_column UInt8
  ) ENGINE = TinyLog;

SHOW CREATE TABLE check_query_comment_column;
DESCRIBE TABLE check_query_comment_column;

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

ALTER TABLE check_query_comment_column
  COMMENT COLUMN first_column 'comment 1_1',
  COMMENT COLUMN second_column 'comment 2_1',
  COMMENT COLUMN third_column 'comment 3_1',
  COMMENT COLUMN fourth_column 'comment 4_1',
  COMMENT COLUMN fifth_column 'comment 5_1';

SHOW CREATE TABLE check_query_comment_column;

ALTER TABLE check_query_comment_column
  MODIFY COLUMN first_column COMMENT 'comment 1_2',
  MODIFY COLUMN second_column COMMENT 'comment 2_2',
  MODIFY COLUMN third_column COMMENT 'comment 3_2',
  MODIFY COLUMN fourth_column COMMENT 'comment 4_2',
  MODIFY COLUMN fifth_column COMMENT 'comment 5_2';

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

SHOW CREATE TABLE check_query_comment_column;
DROP TABLE IF EXISTS check_query_comment_column;

-- Check `ALTER TABLE table_name COMMENT COLUMN 'comment'` statement with MergeTree engine
CREATE TABLE check_query_comment_column
  (
    first_column UInt8 COMMENT 'comment 1',
    second_column UInt8 COMMENT 'comment 2',
    third_column UInt8 COMMENT 'comment 3'
  ) ENGINE = MergeTree()
        ORDER BY first_column
        PARTITION BY second_column
        SAMPLE BY first_column;

SHOW CREATE TABLE check_query_comment_column;
DESCRIBE TABLE check_query_comment_column;

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

ALTER TABLE check_query_comment_column
  COMMENT COLUMN first_column 'comment 1_2',
  COMMENT COLUMN second_column 'comment 2_2',
  COMMENT COLUMN third_column 'comment 3_2';

SHOW CREATE TABLE check_query_comment_column;

ALTER TABLE check_query_comment_column
  MODIFY COLUMN first_column COMMENT 'comment 1_3',
  MODIFY COLUMN second_column COMMENT 'comment 2_3',
  MODIFY COLUMN third_column COMMENT 'comment 3_3';

SHOW CREATE TABLE check_query_comment_column;

ALTER TABLE check_query_comment_column
  MODIFY COLUMN first_column DEFAULT 1 COMMENT 'comment 1_3',
  MODIFY COLUMN second_column COMMENT 'comment 2_3',            -- We can't change default value of partition key.
  MODIFY COLUMN third_column DEFAULT 1 COMMENT 'comment 3_3';

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' and database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

DROP TABLE IF EXISTS check_query_comment_column;

-- TODO: add here tests with ReplicatedMergeTree
