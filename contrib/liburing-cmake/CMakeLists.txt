set (ENABLE_LIBURING_DEFAULT ${ENABLE_LIBRARIES})

if (NOT OS_LINUX)
    set (ENABLE_LIBURING_DEFAULT OFF)
endif ()

option (ENABLE_LIBURING "Enable liburing" ${ENABLE_LIBURING_DEFAULT})

if (NOT ENABLE_LIBURING)
    message(STATUS "Not using liburing")
    return()
endif()

set (LIBURING_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/liburing/src/include")
set (LIBURING_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/liburing/src")

set (SRCS
    "${LIBURING_SOURCE_DIR}/queue.c"
    "${LIBURING_SOURCE_DIR}/register.c"
    "${LIBURING_SOURCE_DIR}/setup.c"
    "${LIBURING_SOURCE_DIR}/syscall.c"
)

set (LIBURING_COMPAT_INCLUDE_DIR "${ClickHouse_BINARY_DIR}/contrib/liburing/src/include-compat")
set (LIBURING_COMPAT_HEADER "${LIBURING_COMPAT_INCLUDE_DIR}/liburing/compat.h")

set (LIBURING_CONFIG_HAS_KERNEL_RWF_T,    "yes")
set (LIBURING_CONFIG_HAS_KERNEL_TIMESPEC, "no")
set (LIBURING_CONFIG_HAS_OPEN_HOW,        "no")
set (LIBURING_CONFIG_HAS_STATX,           "no")
set (LIBURING_CONFIG_HAS_GLIBC_STATX,     "no")

add_compile_definitions(_GNU_SOURCE)
add_compile_definitions(LIBURING_INTERNAL)

configure_file (compat.h.in ${LIBURING_COMPAT_HEADER})

add_library (_liburing ${SRCS})
add_library (ch_contrib::liburing ALIAS _liburing)

target_include_directories (_liburing SYSTEM PUBLIC ${LIBURING_COMPAT_INCLUDE_DIR} "${LIBURING_SOURCE_DIR}/include")
