set (CLICKHOUSE_STATIC_FILES_DISK_UPLOADER_SOURCES clickhouse-static-files-disk-uploader.cpp)

set (CLICKHOUSE_STATIC_FILES_DISK_UPLOADER_LINK
    PRIVATE
        boost::program_options
        clickhouse_common_io
        dbms
)

clickhouse_program_add(static-files-disk-uploader)
