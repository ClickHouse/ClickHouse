-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Read single file from a single 7z archive
SELECT id, data, _file, _path FROM s3(s3_conn, filename='04040_archive1.7z :: example1.csv', format='CSV', structure='id UInt32, data String') ORDER BY (id, _file, _path);

-- Read all files from a single 7z archive using glob
SELECT id, data, _file, _path FROM s3(s3_conn, filename='04040_archive1.7z :: example*.csv', format='CSV', structure='id UInt32, data String') ORDER BY (id, _file, _path);

-- Read single file from multiple 7z archives using glob
SELECT id, data, _file, _path FROM s3(s3_conn, filename='04040_archive*.7z :: example2.csv', format='CSV', structure='id UInt32, data String') ORDER BY (id, _file, _path);

-- Read all files from all 7z archives
SELECT id, data, _file, _path FROM s3(s3_conn, filename='04040_archive*.7z :: example*', format='CSV', structure='id UInt32, data String') ORDER BY (id, _file, _path);

-- Nonexistent file inside archive
SELECT id, data FROM s3(s3_conn, filename='04040_archive1.7z :: nonexistent.csv'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
