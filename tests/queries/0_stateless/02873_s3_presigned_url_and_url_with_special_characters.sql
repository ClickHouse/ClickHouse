-- Tags: no-fasttest

select * from s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/MyPrefix/BU%20-%20UNIT%20-%201/*.parquet'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

select * from s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/MyPrefix/*.parquet?some_tocken=ABCD'); -- { serverError CANNOT_DETECT_FORMAT }
