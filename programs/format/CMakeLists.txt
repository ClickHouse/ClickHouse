set (CLICKHOUSE_FORMAT_SOURCES Format.cpp)

set (CLICKHOUSE_FORMAT_LINK
    PRIVATE
        boost::program_options
        clickhouse_common_io
        clickhouse_parsers
        dbms
)

clickhouse_program_add(format)
