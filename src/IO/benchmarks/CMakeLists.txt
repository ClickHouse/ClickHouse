clickhouse_add_microbenchmark (s3_upload_checksum_staging
    SOURCES s3_upload_checksum_staging.cpp
    LIBRARIES
        clickhouse_common_io
        dbms)
