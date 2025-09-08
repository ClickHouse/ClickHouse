include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_parsers .)
add_headers_and_sources(clickhouse_parsers ./Access)
add_headers_and_sources(clickhouse_parsers ./MySQL)
add_headers_and_sources(clickhouse_parsers ./Kusto)
add_headers_and_sources(clickhouse_parsers ./PRQL)
add_headers_and_sources(clickhouse_parsers ./Kusto/KustoFunctions)
add_library(clickhouse_parsers ${clickhouse_parsers_headers} ${clickhouse_parsers_sources})
target_link_libraries(clickhouse_parsers PUBLIC clickhouse_common_io clickhouse_common_access)
if (TARGET ch_rust::prql)
    target_link_libraries(clickhouse_parsers PRIVATE ch_rust::prql)
endif ()

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()

if (ENABLE_LEXER_TEST)
    set(LEXER_STANDALONE_BUILD ON CACHE BOOL "Enable standalone lexer build")
endif ()

# Lexer standalone library
if (LEXER_STANDALONE_BUILD)
    add_library(lexer-c Lexer.cpp)
    target_include_directories(lexer-c PRIVATE ..)
    target_compile_options(lexer-c PRIVATE -Os -fno-exceptions -fno-rtti -nostdlib -DLEXER_STANDALONE_BUILD)
endif ()
