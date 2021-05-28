include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_parsers .)
add_headers_and_sources(clickhouse_parsers ./MySQL)
add_library(clickhouse_parsers ${clickhouse_parsers_headers} ${clickhouse_parsers_sources})
target_link_libraries(clickhouse_parsers PUBLIC clickhouse_common_io)

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-I\"${ClickHouse_SOURCE_DIR}/base\" -include \"${ClickHouse_SOURCE_DIR}/src/Parsers/iostream_debug_helpers.h\"")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
