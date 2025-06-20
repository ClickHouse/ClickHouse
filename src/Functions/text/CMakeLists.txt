include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
include(CMakePrintHelpers)

add_headers_and_sources(clickhouse_functions_text .)

add_library(clickhouse_functions_text OBJECT ${clickhouse_functions_text_sources} ${clickhouse_functions_text_headers})
target_link_libraries(clickhouse_functions_text PRIVATE dbms)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_text PRIVATE "-g0")
endif()
