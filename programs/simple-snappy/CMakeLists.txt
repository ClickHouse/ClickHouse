set (CLICKHOUSE_SIMPLE_SNAPPY_SOURCES simple-snappy.cpp)

set (CLICKHOUSE_SIMPLE_SNAPPY_SOURCES_LINK
    PRIVATE
    dbms
)

clickhouse_program_add(simple-snappy)

target_include_directories (clickhouse-simple-snappy-lib PRIVATE "${PROJECT_SOURCE_DIR}/contrib/snappy" "${CMAKE_CURRENT_BINARY_DIR}/../../contrib/snappy-cmake")
