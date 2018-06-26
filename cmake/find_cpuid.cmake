option (USE_INTERNAL_CPUID_LIBRARY "Set to FALSE to use system cpuid library instead of bundled" ${NOT_UNBUNDLED})

#if (USE_INTERNAL_CPUID_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libcpuid/include/cpuid/libcpuid.h")
#   message (WARNING "submodule contrib/libcpuid is missing. to fix try run: \n git submodule update --init --recursive")
#   set (USE_INTERNAL_CPUID_LIBRARY 0)
#endif ()

if (NOT USE_INTERNAL_CPUID_LIBRARY)
    find_library (CPUID_LIBRARY cpuid)
    find_path (CPUID_INCLUDE_DIR NAMES libcpuid/libcpuid.h PATHS ${CPUID_INCLUDE_PATHS})
endif ()

if (CPUID_LIBRARY AND CPUID_INCLUDE_DIR)
else ()
    set (CPUID_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libcpuid/include)
    set (USE_INTERNAL_CPUID_LIBRARY 1)
    set (CPUID_LIBRARY cpuid)
endif ()

message (STATUS "Using cpuid: ${CPUID_INCLUDE_DIR} : ${CPUID_LIBRARY}")
