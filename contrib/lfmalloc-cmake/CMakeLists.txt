option (ENABLE_LFMALLOC "Enable lfmalloc allocator" ON)

if (NOT ENABLE_LFMALLOC)
    message (STATUS "Not using lfmalloc")
    return()
endif ()


if (ENABLE_JEMALLOC)
    message (FATAL_ERROR "Cannot enable both lfmalloc and jemalloc")
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/catboost/")

set (SRCS
    "${LIBRARY_DIR}/library/cpp/lfalloc/lf_allocX64.h"
)

add_library(_lfmalloc ${SRCS})

target_compile_definitions(_lfmalloc PRIVATE ENABLE_STATISTICS ENABLE_OVERRIDE ENABLE_PRELOAD _GNU_SOURCE)
target_include_directories(_lfmalloc SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::lfmalloc ALIAS _lfmalloc)
