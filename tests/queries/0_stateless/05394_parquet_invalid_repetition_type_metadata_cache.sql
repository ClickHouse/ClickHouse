-- Tags: no-fasttest
-- no-fasttest: depends on s3 storage

-- An out-of-range `repetition_type` in the Parquet footer must be rejected before the metadata
-- enters the Parquet metadata cache: copying it out of the cache loads the invalid enum value.

SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = '05394_good.parquet', format = Parquet)
SELECT 1::Int32 AS bad_repetition_type;

-- In the footer, the element of the column is: `type` = INT32, `repetition_type` = REQUIRED
-- (field header 0x25, value 0), `name`. Replace the zigzag-encoded value 0 with 1, which is -1.
INSERT INTO FUNCTION s3(s3_conn, filename = '05394_bad.parquet', format = RawBLOB)
SELECT replaceOne(raw, unhex('25001813') || 'bad_repetition_type', unhex('25011813') || 'bad_repetition_type')
FROM s3(s3_conn, filename = '05394_good.parquet', format = RawBLOB, structure = 'raw String');

SELECT * FROM s3(s3_conn, filename = '05394_bad.parquet', format = Parquet, structure = 'bad_repetition_type Int32')
SETTINGS use_parquet_metadata_cache = 1; -- { serverError INCORRECT_DATA }
SELECT * FROM s3(s3_conn, filename = '05394_bad.parquet', format = Parquet, structure = 'bad_repetition_type Int32')
SETTINGS use_parquet_metadata_cache = 1; -- { serverError INCORRECT_DATA }
SELECT count() FROM s3(s3_conn, filename = '05394_bad.parquet', format = Parquet, structure = 'bad_repetition_type Int32')
SETTINGS use_parquet_metadata_cache = 1; -- { serverError INCORRECT_DATA }
DESCRIBE s3(s3_conn, filename = '05394_bad.parquet', format = Parquet)
SETTINGS use_parquet_metadata_cache = 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

SELECT * FROM s3(s3_conn, filename = '05394_good.parquet', format = Parquet)
SETTINGS use_parquet_metadata_cache = 1;
