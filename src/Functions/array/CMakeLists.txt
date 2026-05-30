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
target_link_libraries(clickhouse_functions_array PRIVATE dbms clickhouse_functions_gatherutils ch_contrib::stringzilla)

if (TARGET ch_contrib::vectorscan)
    target_link_libraries(clickhouse_functions_array PRIVATE ch_contrib::vectorscan)
endif()

# Libcxx hardening means out-of-bounds checks which causes noticeable degradations for some workflows.
# TUs where libcxx hardening needs to be disabled are specified here.
set_source_files_properties(array.cpp PROPERTIES COMPILE_FLAGS "-U_LIBCPP_HARDENING_MODE -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_NONE")

# The project sets `-ffp-contract=off` globally (see top-level CMakeLists.txt) for numerical determinism.
# Re-enable FMA fusion only for these two TUs: the dot-product and distance kernels rely on the compiler
# generating `vfmadd*` from `a*b + c` patterns in their hot inner loops. The per-file `COMPILE_FLAGS` are
# appended after `CMAKE_CXX_FLAGS`, so this override wins over the global `-ffp-contract=off`.
set_source_files_properties(
    arrayDotProduct.cpp arrayDistance.cpp
    PROPERTIES COMPILE_FLAGS "-ffp-contract=fast")

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_array PRIVATE "-g1")
endif()
