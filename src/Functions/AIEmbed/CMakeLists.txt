include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_functions_ai_embed .)

add_library(clickhouse_functions_ai_embed OBJECT ${clickhouse_functions_ai_embed_sources} ${clickhouse_functions_ai_embed_headers})
target_link_libraries(clickhouse_functions_ai_embed PRIVATE dbms)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_ai_embed PRIVATE "-g1")
endif()
