-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

DROP TABLE IF EXISTS archive_setting_s3_literal;
DROP TABLE IF EXISTS archive_setting_s3_archive;
DROP TABLE IF EXISTS archive_setting_s3_queue;
DROP TABLE IF EXISTS archive_setting_url_archive;

INSERT INTO FUNCTION s3(
    s3_conn,
    filename = currentDatabase() || '/04893/literal.zip::data.csv',
    format = 'CSV',
    structure = 'x UInt64')
SETTINGS allow_archive_path_syntax = 0, s3_truncate_on_insert = 1
SELECT 33;

SET allow_archive_path_syntax = 0;
CREATE TABLE archive_setting_s3_literal (x UInt64)
ENGINE = S3(
    s3_conn,
    filename = currentDatabase() || '/04893/literal.zip::data.csv',
    format = 'CSV');

SELECT create_table_query LIKE '%allow_archive_path_syntax = false%'
FROM system.tables
WHERE database = currentDatabase() AND name = 'archive_setting_s3_literal';
SELECT * FROM archive_setting_s3_literal;

SET allow_archive_path_syntax = 1;
DETACH TABLE archive_setting_s3_literal;
ATTACH TABLE archive_setting_s3_literal;
SELECT * FROM archive_setting_s3_literal;
DROP TABLE archive_setting_s3_literal;

SET allow_archive_path_syntax = 1;
CREATE TABLE archive_setting_s3_archive (id UInt64, data String)
ENGINE = S3(s3_conn, filename = '03036_archive1.zip::example1.csv', format = 'CSV');

SELECT create_table_query LIKE '%allow_archive_path_syntax = true%'
FROM system.tables
WHERE database = currentDatabase() AND name = 'archive_setting_s3_archive';
SELECT arraySort(groupArray(id)) FROM archive_setting_s3_archive;

SET allow_archive_path_syntax = 0;
DETACH TABLE archive_setting_s3_archive;
ATTACH TABLE archive_setting_s3_archive;
SELECT arraySort(groupArray(id)) FROM archive_setting_s3_archive;
DROP TABLE archive_setting_s3_archive;

SET allow_experimental_url_wildcard_from_index_pages = 1;
SET allow_archive_path_syntax = 1;
CREATE TABLE archive_setting_url_archive (id UInt64, data String)
ENGINE = URL('http://localhost:11111/test/03036_archive1.zip::example*.csv', 'CSV');

SELECT create_table_query LIKE '%allow_archive_path_syntax = true%'
FROM system.tables
WHERE database = currentDatabase() AND name = 'archive_setting_url_archive';
SELECT arraySort(groupArray(id)) FROM archive_setting_url_archive;

DETACH TABLE archive_setting_url_archive;
SET allow_archive_path_syntax = 0;
ATTACH TABLE archive_setting_url_archive;
SELECT arraySort(groupArray(id)) FROM archive_setting_url_archive;
DROP TABLE archive_setting_url_archive;

SET allow_archive_path_syntax = 0;
CREATE TABLE archive_setting_s3_queue (x UInt64)
ENGINE = S3Queue(
    s3_conn,
    filename = currentDatabase() || '/04893/queue_literal.zip::*.csv',
    format = 'CSV')
SETTINGS mode = 'unordered';

SELECT create_table_query LIKE '%allow_archive_path_syntax = false%'
FROM system.tables
WHERE database = currentDatabase() AND name = 'archive_setting_s3_queue';

DETACH TABLE archive_setting_s3_queue;
SET allow_archive_path_syntax = 1;
ATTACH TABLE archive_setting_s3_queue;
SELECT create_table_query LIKE '%allow_archive_path_syntax = false%'
FROM system.tables
WHERE database = currentDatabase() AND name = 'archive_setting_s3_queue';
DROP TABLE archive_setting_s3_queue SYNC;
