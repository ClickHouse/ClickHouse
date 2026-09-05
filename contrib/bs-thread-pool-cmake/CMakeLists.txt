# BS::thread_pool, used by cuDF's host-side IO. Header-only.

if (NOT ENABLE_GPU)
    message (STATUS "Not using BS::thread_pool")
    return ()
endif ()

add_library (_bs_thread_pool INTERFACE)
target_include_directories (_bs_thread_pool SYSTEM INTERFACE
    "${ClickHouse_SOURCE_DIR}/contrib/bs-thread-pool/include")
add_library (ch_contrib::bs_thread_pool ALIAS _bs_thread_pool)
