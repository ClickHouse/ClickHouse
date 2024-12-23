## accel_config is the utility library required by QPL-Deflate codec for controlling and configuring Intel® In-Memory Analytics Accelerator (Intel® IAA).
set (LIBACCEL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/idxd-config")
set (UUID_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl-cmake")
set (LIBACCEL_HEADER_DIR "${ClickHouse_SOURCE_DIR}/contrib/idxd-config-cmake/include")
set (SRCS
    "${LIBACCEL_SOURCE_DIR}/accfg/lib/libaccfg.c"
    "${LIBACCEL_SOURCE_DIR}/util/log.c"
    "${LIBACCEL_SOURCE_DIR}/util/sysfs.c"
)

add_library(_accel-config ${SRCS})

target_compile_options(_accel-config PRIVATE "-D_GNU_SOURCE")

target_include_directories(_accel-config BEFORE
    PRIVATE ${UUID_DIR}
    PRIVATE ${LIBACCEL_HEADER_DIR}
    PRIVATE ${LIBACCEL_SOURCE_DIR})

target_include_directories(_accel-config SYSTEM BEFORE
    PUBLIC ${LIBACCEL_SOURCE_DIR}/accfg)

add_library(ch_contrib::accel-config ALIAS _accel-config)
