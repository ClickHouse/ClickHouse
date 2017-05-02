include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_functions .)
add_headers_and_sources(clickhouse_functions ./Conditional)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp DataTypeTraits.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h DataTypeTraits.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
