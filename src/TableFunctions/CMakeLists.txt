include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_table_functions .)
if (TARGET ch_contrib::hivemetastore)
    add_headers_and_sources(clickhouse_table_functions Hive)
endif ()

list(REMOVE_ITEM clickhouse_table_functions_sources ITableFunction.cpp TableFunctionFactory.cpp)
list(REMOVE_ITEM clickhouse_table_functions_headers ITableFunction.h TableFunctionFactory.h)

add_library(clickhouse_table_functions ${clickhouse_table_functions_sources})
if (TARGET ch_contrib::hivemetastore)
    target_link_libraries(clickhouse_table_functions PRIVATE clickhouse_parsers clickhouse_storages_system dbms ch_contrib::hivemetastore ch_contrib::hdfs)
else ()
    target_link_libraries(clickhouse_table_functions PRIVATE clickhouse_parsers clickhouse_storages_system dbms)
endif ()


