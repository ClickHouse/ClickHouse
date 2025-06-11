-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select * from icebergS3(s3_conn, filename='est') limit 10;