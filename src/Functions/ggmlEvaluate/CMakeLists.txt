include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

if (TARGET ch_contrib::ggml)
    add_headers_and_sources(clickhouse_functions_ggmlEvaluate .)
    add_headers_and_sources(clickhouse_functions_ggmlEvaluate models)

    add_library(clickhouse_functions_ggmlEvaluate ${clickhouse_functions_ggmlEvaluate_sources} ${clickhouse_functions_ggmlEvaluate_headers})

    target_link_libraries(clickhouse_functions_ggmlEvaluate PRIVATE dbms)

    target_link_libraries(clickhouse_functions_ggmlEvaluate PRIVATE ch_contrib::ggml)
endif()
