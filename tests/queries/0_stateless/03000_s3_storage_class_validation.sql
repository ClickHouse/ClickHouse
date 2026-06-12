-- Tags: no-fasttest
SELECT 'Testing invalid S3 storage classes';
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', storage_class_name='FSX_OPENZFS'); -- { serverError INVALID_SETTING_VALUE }
SELECT * FROM s3('http://localhost:11111/test/bucket', 'CSV', 'x String', storage_class_name='INVALID_CLASS'); -- { serverError INVALID_SETTING_VALUE }
