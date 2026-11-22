-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive1.zip :: example1.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive1.zip :: example1.csv') WHERE _file = 'example1.csv' ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive1.zip :: example1.csv') WHERE _file GLOBAL IN (SELECT 'example1.csv') ORDER BY (id, _file, _path);
SELECT count() FROM s3(s3_conn, filename='03036_missing_archive.zip :: entry.csv', format='CSV', structure='id UInt64') WHERE _file GLOBAL IN (SELECT 'not_entry.csv');
SELECT count() FROM s3(s3_conn, filename='{03036_missing_archive.zip,03036_archive1.zip} :: entry.csv', format='CSV', structure='id UInt64') WHERE _file GLOBAL IN (SELECT 'not_entry.csv');
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive2.zip :: example*.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.zip :: example2.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.zip :: example2.csv') WHERE _file = 'example2.csv' ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.zip :: example2.csv') WHERE _file GLOBAL IN (SELECT 'example2.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.zip :: example*') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive1.tar :: example1.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.tar :: example4.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive2.tar :: example*.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.tar.gz :: example*.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.tar* :: example{2..3}.csv') ORDER BY (id, _file, _path);
select id, data, _size, _file, _path from s3(s3_conn, filename='03036_archive2.zip :: nonexistent.csv'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
select id, data, _size, _file, _path from s3(s3_conn, filename='03036_archive2.zip :: nonexistent{2..3}.csv'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
CREATE TABLE table_zip22 Engine S3(s3_conn, filename='03036_archive2.zip :: example2.csv');
select id, data, _size, _file, _path from table_zip22 ORDER BY (id, _file, _path);
CREATE table table_tar2star Engine S3(s3_conn, filename='03036_archive2.tar :: example*.csv');
SELECT id, data, _size, _file, _path FROM table_tar2star ORDER BY (id, _file, _path);
CREATE table table_tarstarglobs Engine S3(s3_conn, filename='03036_archive*.tar* :: example{2..3}.csv');
SELECT id, data, _size, _file, _path FROM table_tarstarglobs ORDER BY (id, _file, _path);
CREATE table table_noexist Engine s3(s3_conn, filename='03036_archive2.zip :: nonexistent.csv'); -- { serverError UNKNOWN_STORAGE }
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_compressed_file_archive.zip :: example7.csv', format='CSV', structure='auto', compression_method='gz') ORDER BY (id, _file, _path);

-- Hive partition columns are inferred from a sample path built during analysis. For an explicit archive
-- member the path string itself carries them (`<archive path>::<path in archive>`), so a missing archive
-- excluded by the `_path` / `_file` predicate must return no rows instead of losing the `date` column and
-- throwing UNKNOWN_IDENTIFIER. The cluster variant infers the columns on a remote node, which is a
-- separate code path, and a pure brace expansion of the archive paths must work the same way.
SELECT id, date FROM s3(s3_conn, filename='date=2026-08-21/03036_missing_archive.zip :: entry.csv', format='CSV', structure='id UInt64') WHERE _file GLOBAL IN (SELECT 'not_entry.csv') SETTINGS use_hive_partitioning = 1;
SELECT id, date FROM s3Cluster('test_shard_localhost', s3_conn, filename='date=2026-08-21/03036_missing_archive.zip :: entry.csv', format='CSV', structure='id UInt64') WHERE _file GLOBAL IN (SELECT 'not_entry.csv') SETTINGS use_hive_partitioning = 1;
SELECT id, date FROM s3Cluster('test_shard_localhost', s3_conn, filename='{date=2026-08-21/03036_missing_archive.zip,date=2026-08-22/03036_missing_archive2.zip} :: entry.csv', format='CSV', structure='id UInt64') WHERE _file GLOBAL IN (SELECT 'not_entry.csv') SETTINGS use_hive_partitioning = 1;

-- The `_path` extraction fast path (`s3_path_filter_limit`) must not apply to archives: the extracted
-- values are entry virtual paths (`<archive>::<member>`), not object keys, so they either fail the
-- outer-glob validation (dropping every archive) or feed a bogus object key into `KeysIterator` when
-- the outer glob happens to match the full entry string (e.g. `*.tar*`).
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.zip :: example2.csv') WHERE _path = 'test/03036_archive1.zip::example2.csv' ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.zip :: example2.csv') WHERE _path IN ('test/03036_archive1.zip::example2.csv', 'test/03036_archive2.zip::example2.csv') ORDER BY (id, _file, _path);
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.tar* :: example{2..3}.csv') WHERE _path = 'test/03036_archive3.tar.gz::example2.csv' ORDER BY (id, _file, _path);

-- A glob in the archive member name must not disable hive partition inference either: the partition columns
-- live in the directory part of the outer archive path, which is known without opening the archive (only the
-- member name needs enumeration, and `parseHivePartitioningKeysAndValues` ignores the file name part anyway).
-- So a missing archive must fail with the storage error for that object rather than lose the `date` column and
-- throw UNKNOWN_IDENTIFIER during analysis. The cluster variant infers the columns on a remote node, which is
-- a separate code path, and a pure brace expansion of the archive paths must work the same way.
SELECT id, date FROM s3(s3_conn, filename='date=2026-08-21/03036_missing_archive.zip :: *.csv', format='CSV', structure='id UInt64') SETTINGS use_hive_partitioning = 1; -- { serverError S3_ERROR }
SELECT id, date FROM s3Cluster('test_shard_localhost', s3_conn, filename='date=2026-08-21/03036_missing_archive.zip :: *.csv', format='CSV', structure='id UInt64') SETTINGS use_hive_partitioning = 1; -- { serverError S3_ERROR }
SELECT id, date FROM s3(s3_conn, filename='{date=2026-08-21/03036_missing_archive.zip,date=2026-08-22/03036_missing_archive2.zip} :: *.csv', format='CSV', structure='id UInt64') SETTINGS use_hive_partitioning = 1; -- { serverError S3_ERROR }
