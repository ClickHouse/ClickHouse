option(USE_INTERNAL_CPUINFO_LIBRARY "Set to FALSE to use system cpuinfo library instead of bundled" ${NOT_UNBUNDLED})

# Now we have no contrib/libcpuinfo, use from system.
if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libcpuinfo/include")
   #message (WARNING "submodule contrib/libcpuinfo is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_CPUINFO_LIBRARY 0)
   set (MISSING_INTERNAL_CPUINFO_LIBRARY 1)
endif ()

if(NOT USE_INTERNAL_CPUINFO_LIBRARY)
    find_library(CPUINFO_LIBRARY cpuinfo)
    find_path(CPUINFO_INCLUDE_DIR NAMES cpuinfo.h PATHS ${CPUINFO_INCLUDE_PATHS})
endif()

if(CPUINFO_LIBRARY AND CPUINFO_INCLUDE_DIR)
    set(USE_CPUINFO 1)
elseif(NOT MISSING_INTERNAL_CPUINFO_LIBRARY)
    set(CPUINFO_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libcpuinfo/include)
    set(USE_INTERNAL_CPUINFO_LIBRARY 1)
    set(CPUINFO_LIBRARY cpuinfo)
    set(USE_CPUINFO 1)
endif()

message(STATUS "Using cpuinfo=${USE_CPUINFO}: ${CPUINFO_INCLUDE_DIR} : ${CPUINFO_LIBRARY}")
