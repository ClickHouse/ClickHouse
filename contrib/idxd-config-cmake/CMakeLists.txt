## Utility library for controlling and configuring Intel® In-Memory Analytics Accelerator (Intel® IAA).
## This is to build libaccel-config which is only required by libqpl (./contrib/qpl).Hence, If ENABLE_QPL=False, this module should not be build as well.
if (NOT ENABLE_QPL)
    message(STATUS "Not using idxd-config due to ENABLE_QPL=False")
    return()
endif()

set(LIBACCEL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/idxd-config")
set(LIBACCEL_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/idxd-config")
set(UUID_DIR "${ClickHouse_SOURCE_DIR}/contrib/idxd-config-cmake")
set(LIBACCEL_HEADER_DIR "${ClickHouse_SOURCE_DIR}/contrib/idxd-config-cmake/idxd-header")
set(SRCS
    "${LIBACCEL_SOURCE_DIR}/accfg/lib/libaccfg.c"
    "${LIBACCEL_SOURCE_DIR}/util/log.c"
    "${LIBACCEL_SOURCE_DIR}/util/sysfs.c"
)

add_library(_libaccel ${SRCS})

target_compile_options(_libaccel PRIVATE "-D_GNU_SOURCE")

target_include_directories(_libaccel BEFORE
    PRIVATE ${UUID_DIR}
    PRIVATE ${LIBACCEL_HEADER_DIR}
    PRIVATE ${LIBACCEL_SOURCE_DIR})

add_library(ch_contrib::libaccel ALIAS _libaccel)
