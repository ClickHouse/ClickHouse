# Also in utils

set (CLICKHOUSE_COMPRESSOR_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Compressor.cpp)

set (CLICKHOUSE_COMPRESSOR_LINK
    PRIVATE
        boost::program_options
        clickhouse_parsers
        dbms
)

clickhouse_program_add(compressor)
