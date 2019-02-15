# Freebsd: TODO: use system devel/xxhash. now error: undefined reference to `XXH32'
if (LZ4_INCLUDE_DIR AND NOT OS_FREEBSD)
    if (NOT EXISTS "${LZ4_INCLUDE_DIR}/xxhash.h")
        message (WARNING "LZ4 library does not have XXHash. Support for XXHash will be disabled.")
        set (USE_XXHASH 0)
    else ()
        set (USE_XXHASH 1)
    endif ()
endif ()

message (STATUS "Using xxhash=${USE_XXHASH}")
