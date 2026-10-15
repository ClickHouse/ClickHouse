# NVTX3 profiler annotations. Header-only; a no-op unless a profiler is attached.

if (NOT ENABLE_GPU)
    message (STATUS "Not using NVTX")
    return ()
endif ()

add_library (_nvtx3 INTERFACE)
target_include_directories (_nvtx3 SYSTEM INTERFACE
    "${ClickHouse_SOURCE_DIR}/contrib/nvtx/c/include")
add_library (ch_contrib::nvtx3 ALIAS _nvtx3)
