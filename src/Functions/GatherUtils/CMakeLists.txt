include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_functions_gatherutils .)
add_library(clickhouse_functions_gatherutils ${clickhouse_functions_gatherutils_sources} ${clickhouse_functions_gatherutils_headers})
target_link_libraries(clickhouse_functions_gatherutils PRIVATE dbms)

if (HAS_SUGGEST_OVERRIDE)
    target_compile_definitions(clickhouse_functions_gatherutils PUBLIC HAS_SUGGEST_OVERRIDE)
endif()

if (HAS_SUGGEST_DESTRUCTOR_OVERRIDE)
    target_compile_definitions(clickhouse_functions_gatherutils PUBLIC HAS_SUGGEST_DESTRUCTOR_OVERRIDE)
endif()

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions_gatherutils PRIVATE "-g0")
endif()

set_target_properties(clickhouse_functions_gatherutils PROPERTIES COMPILE_FLAGS "${X86_INTRINSICS_FLAGS}")
