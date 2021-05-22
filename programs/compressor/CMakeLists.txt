# Also in utils

set (CLICKHOUSE_COMPRESSOR_SOURCES Compressor.cpp)

set (CLICKHOUSE_COMPRESSOR_LINK
    PRIVATE
        boost::program_options
        dbms
)

clickhouse_program_add(compressor)
