if (NOT (
    OS_LINUX AND (ARCH_AMD64 OR ARCH_AARCH64 OR ARCH_LOONGARCH64))
)
    if (ENABLE_NUMACTL)
        message (${RECONFIGURE_MESSAGE_LEVEL}
            "numactl is disabled implicitly because the OS or architecture is not supported. Use -DENABLE_NUMACTL=0")
    endif ()
    set (ENABLE_NUMACTL OFF)
else()
    option (ENABLE_NUMACTL "Enable numactl" ${ENABLE_LIBRARIES})
endif()

if (NOT ENABLE_NUMACTL)
    message (STATUS "Not using numactl")
    return()
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/numactl")

set (SRCS
    "${LIBRARY_DIR}/libnuma.c"
    "${LIBRARY_DIR}/syscall.c"
)

add_library(_numactl ${SRCS})

target_include_directories(_numactl SYSTEM PRIVATE include)
target_include_directories(_numactl SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::numactl ALIAS _numactl)
