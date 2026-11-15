-- Tags: no-fasttest

-- The checksum algorithm arguments are validated, so an invalid value proves the argument is applied.
SELECT 'Invalid algorithms are rejected';
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', upload_checksum_algorithm='MD4'); -- { serverError INVALID_SETTING_VALUE }
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', delete_checksum_algorithm='MD4'); -- { serverError INVALID_SETTING_VALUE }

-- The argument is parsed after the query-level setting, so its value is the one validated.
SELECT 'Argument is applied after the query setting';
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', upload_checksum_algorithm='MD4') SETTINGS s3_upload_checksum_algorithm='SHA256'; -- { serverError INVALID_SETTING_VALUE }

SELECT 'Valid algorithms are accepted in any case';
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', upload_checksum_algorithm='SHA256');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', upload_checksum_algorithm='crc32');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', delete_checksum_algorithm='md5');
