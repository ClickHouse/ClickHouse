option (ENABLE_SNMALLOC "Enable snmalloc allocator" ON)

if (NOT ENABLE_SNMALLOC)
    message (STATUS "Not using snmalloc")
    return()
endif ()


if (ENABLE_JEMALLOC)
    message (FATAL_ERROR "Cannot enable both snmalloc and jemalloc")
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/snmalloc/")

set (SRCS
    "${LIBRARY_DIR}/src/snmalloc/snmalloc.h"
)

add_library(_snmalloc ${SRCS})

target_compile_definitions(_snmalloc PRIVATE ENABLE_STATISTICS ENABLE_OVERRIDE ENABLE_PRELOAD _GNU_SOURCE)
target_include_directories(_snmalloc SYSTEM PUBLIC "${LIBRARY_DIR}")

