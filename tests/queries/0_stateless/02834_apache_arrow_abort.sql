-- Tags: no-fasttest
-- This tests depends on internet access, but it does not matter, because it only has to check that there is no abort due to a bug in Apache Arrow library.

INSERT INTO TABLE FUNCTION url('https://clickhouse-public-datasets.s3.amazonaws.com/hits_compatible/hits.parquet') SELECT * FROM url('https://clickhouse-public-datasets.s3.amazonaws.com/hits_compatible/hits.parquet'); -- { serverError CANNOT_WRITE_TO_OSTREAM, RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, POCO_EXCEPTION }
