DROP TABLE IF EXISTS base;
DROP TABLE IF EXISTS copy_without_comment;
DROP TABLE IF EXISTS copy_with_comment;

CREATE TABLE base (a Int32) ENGINE = MergeTree ORDER BY a COMMENT 'original comment';
CREATE TABLE copy_without_comment as base;
CREATE TABLE copy_with_comment as base COMMENT 'new comment';

SELECT comment FROM system.tables WHERE name = 'base';
SELECT comment FROM system.tables WHERE name = 'copy_without_comment';
SELECT comment FROM system.tables WHERE name = 'copy_with_comment';