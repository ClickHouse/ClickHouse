include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_serializations .)
add_library(clickhouse_serializations ${clickhouse_serializations_sources} ${clickhouse_serializations_headers})
target_link_libraries(clickhouse_serializations PRIVATE dbms clickhouse_functions_gatherutils)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_serializations PRIVATE "-g0")
endif()

if (TARGET ch_contrib::simdjson)
    target_link_libraries(clickhouse_serializations PRIVATE ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    target_link_libraries(clickhouse_serializations PRIVATE ch_contrib::rapidjson)
endif()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
