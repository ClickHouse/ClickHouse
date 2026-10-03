add_library(clickhouse_functions_html OBJECT FunctionHtml.cpp)

target_link_libraries(clickhouse_functions_html PRIVATE dbms ch_contrib::lexbor)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_html PRIVATE "-g0")
endif()
