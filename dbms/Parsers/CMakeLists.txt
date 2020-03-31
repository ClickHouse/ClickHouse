include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_parsers .)
add_library(clickhouse_parsers ${clickhouse_parsers_headers} ${clickhouse_parsers_sources})
target_link_libraries(clickhouse_parsers PUBLIC clickhouse_common_io)
target_include_directories(clickhouse_parsers PUBLIC ${DBMS_INCLUDE_DIR})

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-I${ClickHouse_SOURCE_DIR}/base -include ${ClickHouse_SOURCE_DIR}/dbms/src/Parsers/iostream_debug_helpers.h")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
