-- Tags: no-fasttest

select * from s3('http://localhost:11111/test/MyPrefix/BU%20-%20UNIT%20-%201/*.parquet'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

select * from s3('http://localhost:11111/test/MyPrefix/*.parquet?some_token=ABCD', NOSIGN); -- { serverError CANNOT_DETECT_FORMAT }
