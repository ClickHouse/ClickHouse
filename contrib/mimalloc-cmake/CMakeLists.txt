option (ENABLE_MIMALLOC "Enable mimalloc allocator" ON)

if (NOT ENABLE_MIMALLOC)
    message (STATUS "Not using mimalloc")
    return()
endif ()

# if (ENABLE_JEMALLOC)
#     message (FATAL_ERROR "Cannot enable both mimalloc and jemalloc")
# endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/mimalloc")

set (SRCS
    "${LIBRARY_DIR}/include/mimalloc.h"
)

add_library(_mimalloc ${SRCS})

target_compile_definitions(_mimalloc PRIVATE ENABLE_STATISTICS ENABLE_OVERRIDE ENABLE_PRELOAD _GNU_SOURCE)
target_include_directories(_mimalloc SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::mimalloc ALIAS _mimalloc)
