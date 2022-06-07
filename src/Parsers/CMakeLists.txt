include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_parsers .)
add_headers_and_sources(clickhouse_parsers ./Access)
add_headers_and_sources(clickhouse_parsers ./MySQL)
add_headers_and_sources(clickhouse_parsers ./MySQLCompatibility)
add_headers_and_sources(mysql_parser ./MySQLCompatibility/ANTLR)
add_headers_and_sources(mysql_parser ./MySQLCompatibility/ParserOverlay)

add_library(clickhouse_parsers ${clickhouse_parsers_headers} ${clickhouse_parsers_sources})
add_library(mysql_parser ${mysql_parser_headers} ${mysql_parser_sources})

target_compile_options(mysql_parser PRIVATE -Wno-everything)

target_link_libraries(mysql_parser PUBLIC antlr4-runtime)
target_link_libraries(clickhouse_parsers PUBLIC clickhouse_common_io clickhouse_common_access string_utils mysql_parser)

if (USE_DEBUG_HELPERS)
    # CMake generator expression will do insane quoting when it encounters special character like quotes, spaces, etc.
    # Prefixing "SHELL:" will force it to use the original text.
    set (INCLUDE_DEBUG_HELPERS "SHELL:-I\"${ClickHouse_SOURCE_DIR}/base\" -include \"${ClickHouse_SOURCE_DIR}/src/Parsers/iostream_debug_helpers.h\"")
    # Use generator expression as we don't want to pollute CMAKE_CXX_FLAGS, which will interfere with CMake check system.
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:${INCLUDE_DEBUG_HELPERS}>)
endif ()

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
