# CUTLASS, header-only, for cuVS: its pairwise distances and the fused L2 nearest neighbour of k-means on sm80+.
# In a namespace of cuVS's choosing (as its get_cutlass.cmake sets it), so it cannot clash with another copy.

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
