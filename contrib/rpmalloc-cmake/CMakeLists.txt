option (ENABLE_RPMALLOC "Enable rpmalloc allocator" ON)

if (NOT ENABLE_RPMALLOC)
    message (STATUS "Not using rpmalloc")
    return()
endif ()


if (ENABLE_JEMALLOC)
    message (FATAL_ERROR "Cannot enable both rpmalloc and jemalloc")
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/rpmalloc")

set (SRCS
    "${LIBRARY_DIR}/rpmalloc/rpmalloc.h"
)

add_library(_rpmalloc ${SRCS})

target_compile_definitions(_rpmalloc PRIVATE ENABLE_STATISTICS ENABLE_OVERRIDE ENABLE_PRELOAD _GNU_SOURCE)
target_include_directories(_rpmalloc SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::rpmalloc ALIAS _rpmalloc)

