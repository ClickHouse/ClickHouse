option (ENABLE_ONETBB "Enable oneTBB allocator" ON)

if (NOT ENABLE_ONETBB)
    message (STATUS "Not using oneTBB")
    return()
endif ()


if (ENABLE_JEMALLOC)
    message (FATAL_ERROR "Cannot enable both oneTBB and jemalloc")
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/oneTBB/")

set (SRCS
    "${LIBRARY_DIR}/src/tbbmalloc/tbbmalloc_internal_api.h"
)

add_library(_oneTBB ${SRCS})

target_compile_definitions(_oneTBB PRIVATE ENABLE_STATISTICS ENABLE_OVERRIDE ENABLE_PRELOAD _GNU_SOURCE)
target_include_directories(_oneTBB SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::oneTBB ALIAS _oneTBB)