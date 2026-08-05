include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
include(CMakePrintHelpers)

add_headers_and_sources(clickhouse_functions_array .)

# createInternalFunctionHasOverloadResolver, FunctionArrayResize,  FunctionEmptyArrayToSingle
# Used by ArrayExistsToHasPass.cpp and ArrayJoinAction.cpp
set(DBMS_ARRAY_FUNCTIONS
    arrayResize.cpp
    has.cpp
    emptyArrayToSingle.cpp
)
extract_into_parent_list(clickhouse_functions_array_sources dbms_sources ${DBMS_ARRAY_FUNCTIONS})
extract_into_parent_list(clickhouse_functions_headers dbms_headers arrayResize.h has.h emptyArrayToSingle.h)

add_library(clickhouse_functions_array OBJECT ${clickhouse_functions_array_sources} ${clickhouse_functions_array_headers})
target_link_libraries(clickhouse_functions_array PRIVATE dbms clickhouse_functions_gatherutils)

if (TARGET ch_contrib::vectorscan)
    target_link_libraries(clickhouse_functions_array PRIVATE ch_contrib::vectorscan)
endif()

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_array PRIVATE "-g0")
endif()
