set (ENABLE_LIBURING_DEFAULT ${ENABLE_LIBRARIES})

if (NOT OS_LINUX)
    set (ENABLE_LIBURING_DEFAULT OFF)
endif ()

option (ENABLE_LIBURING "Enable liburing" ${ENABLE_LIBURING_DEFAULT})

if (NOT ENABLE_LIBURING)
    message (STATUS "Not using liburing")
    return ()
endif ()

set (LIBURING_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/liburing/src/include")
set (LIBURING_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/liburing/src")

set (SRCS
    "${LIBURING_SOURCE_DIR}/queue.c"
    "${LIBURING_SOURCE_DIR}/register.c"
    "${LIBURING_SOURCE_DIR}/setup.c"
    "${LIBURING_SOURCE_DIR}/syscall.c"
    "${LIBURING_SOURCE_DIR}/version.c"
)

add_compile_definitions (_GNU_SOURCE)
add_compile_definitions (LIBURING_INTERNAL)

set (LIBURING_COMPAT_INCLUDE_DIR "${ClickHouse_BINARY_DIR}/contrib/liburing/src/include-compat")
set (LIBURING_COMPAT_HEADER "${LIBURING_COMPAT_INCLUDE_DIR}/liburing/compat.h")

set (LIBURING_CONFIG_HAS_KERNEL_RWF_T    FALSE)
set (LIBURING_CONFIG_HAS_KERNEL_TIMESPEC FALSE)
set (LIBURING_CONFIG_HAS_OPEN_HOW        FALSE)
set (LIBURING_CONFIG_HAS_STATX           FALSE)
set (LIBURING_CONFIG_HAS_GLIBC_STATX     FALSE)
set (LIBURING_CONFIG_HAS_DISCARD_CMD     FALSE)

configure_file (compat.h.in ${LIBURING_COMPAT_HEADER})

set (LIBURING_GENERATED_INCLUDE_DIR "${ClickHouse_BINARY_DIR}/contrib/liburing/src/include")
set (LIBURING_VERSION_HEADER "${LIBURING_GENERATED_INCLUDE_DIR}/liburing/io_uring_version.h")

file (READ "${LIBURING_SOURCE_DIR}/../liburing.spec" LIBURING_SPEC)

string (REGEX MATCH "Version: ([0-9]+)\.([0-9]+)" _ ${LIBURING_SPEC})
set (LIBURING_VERSION_MAJOR ${CMAKE_MATCH_1})
set (LIBURING_VERSION_MINOR ${CMAKE_MATCH_2})

configure_file (io_uring_version.h.in ${LIBURING_VERSION_HEADER})

add_library (_liburing ${SRCS})
add_library (ch_contrib::liburing ALIAS _liburing)

target_include_directories (_liburing SYSTEM PUBLIC ${LIBURING_COMPAT_INCLUDE_DIR} ${LIBURING_GENERATED_INCLUDE_DIR} ${LIBURING_INCLUDE_DIR})
