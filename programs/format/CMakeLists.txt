set (CLICKHOUSE_FORMAT_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Format.cpp)

set (CLICKHOUSE_FORMAT_LINK
    PRIVATE
        boost::program_options
        clickhouse_common_io
        clickhouse_parsers
        clickhouse_functions
        clickhouse_aggregate_functions
        clickhouse_table_functions
        dbms
)

clickhouse_program_add(format)
