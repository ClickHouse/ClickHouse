-- Tags: no-fasttest

select * from s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/MyPrefix/BU%20-%20UNIT%20-%201/*.parquet', NOSIGN) settings s3_throw_on_zero_files_match=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

select * from s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/MyPrefix/*.parquet?some_tocken=ABCD', NOSIGN) settings s3_throw_on_zero_files_match=0; -- { serverError CANNOT_DETECT_FORMAT }
