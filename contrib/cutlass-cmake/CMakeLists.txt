# CUTLASS, header-only, for cuVS, in the namespace its get_cutlass.cmake picks.

if (NOT ENABLE_GPU)
    message (STATUS "Not using CUTLASS")
    return ()
endif ()

add_library (_cutlass INTERFACE)
target_include_directories (_cutlass SYSTEM INTERFACE
    "${ClickHouse_SOURCE_DIR}/contrib/cutlass/include"
    "${ClickHouse_SOURCE_DIR}/contrib/cutlass/tools/util/include")
target_compile_definitions (_cutlass INTERFACE CUTLASS_NAMESPACE=cuvs_cutlass)
add_library (ch_contrib::cutlass ALIAS _cutlass)
