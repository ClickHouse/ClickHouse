if(ARCH_AMD64)
    option (ENABLE_CPUID "Enable libcpuid library (only internal)" ${ENABLE_LIBRARIES})
elseif(ENABLE_CPUID)
    message (${RECONFIGURE_MESSAGE_LEVEL} "libcpuid is only supported on x86_64")
    set (ENABLE_CPUID 0)
endif()

if (NOT ENABLE_CPUID)
    message("Not using cpuid")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libcpuid")

set (SRCS
    "${LIBRARY_DIR}/libcpuid/asm-bits.c"
    "${LIBRARY_DIR}/libcpuid/cpuid_main.c"
    "${LIBRARY_DIR}/libcpuid/libcpuid_util.c"
    "${LIBRARY_DIR}/libcpuid/msrdriver.c"
    "${LIBRARY_DIR}/libcpuid/rdmsr.c"
    "${LIBRARY_DIR}/libcpuid/rdtsc.c"
    "${LIBRARY_DIR}/libcpuid/recog_amd.c"
    "${LIBRARY_DIR}/libcpuid/recog_intel.c"
)

add_library (_cpuid ${SRCS})

target_include_directories (_cpuid SYSTEM PUBLIC "${LIBRARY_DIR}")
target_compile_definitions (_cpuid PRIVATE VERSION="v0.4.1")
if (COMPILER_CLANG)
    target_compile_options (_cpuid PRIVATE -Wno-reserved-id-macro)
endif ()

add_library(ch_contrib::cpuid ALIAS _cpuid)
