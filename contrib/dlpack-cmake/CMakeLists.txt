# DLPack tensor interchange headers, for cuDF's to_dlpack/from_dlpack. Header-only.

if (NOT ENABLE_GPU)
    message (STATUS "Not using DLPack")
    return ()
endif ()

add_library (_dlpack INTERFACE)
target_include_directories (_dlpack SYSTEM INTERFACE
    "${ClickHouse_SOURCE_DIR}/contrib/dlpack/include")
add_library (ch_contrib::dlpack ALIAS _dlpack)
