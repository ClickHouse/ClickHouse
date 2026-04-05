include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_functions_gatherutils .)

# Used by arrayResize.cpp
set(DBMS_GATHERUTILS_FUNCTIONS
    createArraySource.cpp
    createValueSource.cpp
    createArraySink.cpp
    resizeConstantSize.cpp
    resizeDynamicSize.cpp
)

extract_into_parent_list(clickhouse_functions_gatherutils_sources dbms_sources ${DBMS_GATHERUTILS_FUNCTIONS})
extract_into_parent_list(clickhouse_functions_gatherutils_headers
    GatherUtils.h
)

add_library(clickhouse_functions_gatherutils ${clickhouse_functions_gatherutils_sources} ${clickhouse_functions_gatherutils_headers})
target_link_libraries(clickhouse_functions_gatherutils PRIVATE dbms)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_gatherutils PRIVATE "-g0")
endif()

set_target_properties(clickhouse_functions_gatherutils PROPERTIES COMPILE_FLAGS "${X86_INTRINSICS_FLAGS}")
