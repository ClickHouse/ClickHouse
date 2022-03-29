include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
option(ENABLE_AVX2 "Enable AVX2 instructions (when available) when build for modern Intel CPUs" OFF)

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

if (HAVE_SSE42)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
    target_compile_options(clickhouse_functions_gatherutils PRIVATE -msse4.2)
endif()
if (HAVE_AVX2 AND ENABLE_AVX2)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2  -DENABLE_AVX2")
    target_compile_options(clickhouse_functions_gatherutils PRIVATE -mavx2)
endif()
