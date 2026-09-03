# Concurrent GPU data structures used by cuDF's hash joins and groupbys. Header-only.

if (NOT ENABLE_GPU)
    message (STATUS "Not using cuCollections")
    return ()
endif ()

add_library (_cuco INTERFACE)
target_include_directories (_cuco SYSTEM INTERFACE
    "${ClickHouse_SOURCE_DIR}/contrib/cuCollections/include")
target_link_libraries (_cuco INTERFACE ch_contrib::cccl)
add_library (ch_contrib::cuco ALIAS _cuco)
