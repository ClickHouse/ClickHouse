include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_ggmlEvaluate .)
add_library(clickhouse_functions_ggmlEvaluate ${clickhouse_functions_ggmlEvaluate_sources} ${clickhouse_functions_ggmlEvaluate_headers})

target_link_libraries(clickhouse_functions_ggmlEvaluate PRIVATE dbms)

if (TARGET ch_contrib::ggml)
    target_link_libraries(clickhouse_functions_ggmlEvaluate PRIVATE ch_contrib::ggml)
endif()
