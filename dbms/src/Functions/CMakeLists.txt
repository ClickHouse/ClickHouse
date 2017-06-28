include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_functions .)
add_headers_and_sources(clickhouse_functions ./Conditional)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp DataTypeTraits.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h DataTypeTraits.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})
target_link_libraries(clickhouse_functions dbms)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libfarmhash)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libmetrohash/src)
target_include_directories (clickhouse_functions BEFORE PUBLIC ${CITYHASH_INCLUDE_DIR})

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
