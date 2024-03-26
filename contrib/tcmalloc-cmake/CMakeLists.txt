option (ENABLE_TCMALLOC "Enable tcmalloc allocator" ON)

if (NOT ENABLE_TCMALLOC)
    message (STATUS "Not using tcmalloc")
    return()
endif ()


if (ENABLE_JEMALLOC)
    message (FATAL_ERROR "Cannot enable both tcmalloc and jemalloc")
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/tcmalloc/")

set (SRCS
    "${LIBRARY_DIR}/tcmalloc/tcmalloc.h"
)

add_library(_tcmalloc ${SRCS})

target_compile_definitions(_tcmalloc PRIVATE ENABLE_STATISTICS ENABLE_OVERRIDE ENABLE_PRELOAD _GNU_SOURCE)
target_include_directories(_tcmalloc SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::tcmalloc ALIAS _tcmalloc)
