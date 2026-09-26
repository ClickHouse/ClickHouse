-- Tags: no-fasttest
SELECT 'Testing invalid S3 storage classes';
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', storage_class_name='FSX_OPENZFS'); -- { serverError INVALID_SETTING_VALUE }
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', storage_class_name='INVALID_CLASS'); -- { serverError INVALID_SETTING_VALUE }

-- The newly accepted classes must pass validation. If the allow-list were reverted to the old
-- {STANDARD, INTELLIGENT_TIERING} set, these queries would throw INVALID_SETTING_VALUE instead of
-- reading the data, so this gives positive regression coverage without depending on an S3 upload.
SELECT 'Testing valid S3 storage classes';
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', storage_class_name='STANDARD');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', storage_class_name='REDUCED_REDUNDANCY');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', storage_class_name='STANDARD_IA');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', storage_class_name='INTELLIGENT_TIERING');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', storage_class_name='ONEZONE_IA');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', storage_class_name='GLACIER_IR');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', storage_class_name='EXPRESS_ONEZONE');

-- Archival classes require an asynchronous restore before an object can be read, so they remain rejected.
SELECT 'Testing rejected archival S3 storage classes';
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', storage_class_name='GLACIER'); -- { serverError INVALID_SETTING_VALUE }
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', storage_class_name='DEEP_ARCHIVE'); -- { serverError INVALID_SETTING_VALUE }
