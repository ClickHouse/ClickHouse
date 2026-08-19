include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_kusto .)
add_library(clickhouse_functions_kusto OBJECT ${clickhouse_functions_kusto_sources} ${clickhouse_functions_kusto_headers})
target_link_libraries(clickhouse_functions_kusto PRIVATE dbms clickhouse_functions_gatherutils)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_kusto PRIVATE "-g0")
endif()
