## Utility library for controlling and configuring Intel® In-Memory Analytics Accelerator (Intel® IAA) available on Next Generation Intel® Xeon® Scalable processors.
set(LIBACCEL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/idxd-config")
set(LIBACCEL_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/idxd-config")
set(UUID_DIR "${ClickHouse_SOURCE_DIR}/contrib/idxd-config-cmake")
set(LIBACCEL_HEADER_DIR "${ClickHouse_SOURCE_DIR}/contrib/idxd-config-cmake/idxd-header")
#set(SYSROOT_HEADER_DIR "${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-x86_64/x86_64-linux-gnu/libc/usr/include")
set(SRCS
    "${LIBACCEL_SOURCE_DIR}/accfg/lib/libaccfg.c"
    "${LIBACCEL_SOURCE_DIR}/util/log.c"
    "${LIBACCEL_SOURCE_DIR}/util/sysfs.c"
)
#"${LIBACCEL_SOURCE_DIR}/util/log.c"
#"${LIBACCEL_SOURCE_DIR}/util/sysfs.c"
add_library(_LIBACCEL ${SRCS})

#target_link_libraries(_LIBACCEL PRIVATE ch_contrib::zlib)
target_compile_options(_LIBACCEL PRIVATE "-D_GNU_SOURCE")

target_include_directories(_LIBACCEL BEFORE
    PRIVATE ${UUID_DIR}
    PRIVATE ${LIBACCEL_HEADER_DIR}
    PRIVATE ${LIBACCEL_SOURCE_DIR})

#target_include_directories(_LIBACCEL BEFORE PUBLIC ${LIBACCEL_BINARY_DIR})

add_library(ch_contrib::LIBACCEL ALIAS _LIBACCEL)
