set(GPERFTOOLS_ROOT ${ClickHouse_SOURCE_DIR}/contrib/gperftools)
set(GPERFTOOLS_BUILD ${ClickHouse_BINARY_DIR}/contrib/gperftools)

set(CC_FULL "${CMAKE_C_COMPILER_LAUNCHER} ${CMAKE_C_COMPILER}")
set(CXX_FULL "${CMAKE_CXX_COMPILER_LAUNCHER} ${CMAKE_CXX_COMPILER}")
get_property(C_LAUNCH GLOBAL PROPERTY RULE_LAUNCH_COMPILE)

include(ExternalProject)
ExternalProject_Add(
    gperftools
    DOWNLOAD_COMMAND ""
    CONFIGURE_COMMAND autoreconf -i ${GPERFTOOLS_ROOT} && env "CC=${C_LAUNCH} ${CMAKE_C_COMPILER}" "CXX=${C_LAUNCH} ${CMAKE_CXX_COMPILER}" CFLAGS=${FULL_C_FLAGS} CXXFLAGS=${FULL_CXX_FLAGS} ${GPERFTOOLS_ROOT}/configure --enable-minimal --with-pic --enable-shared=no --prefix=${GPERFTOOLS_BUILD}
    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j `nproc || sysctl -n hw.ncpu || echo 4`
)

set (GPERFTOOLS_TCMALLOC_MINIMAL ${ClickHouse_BINARY_DIR}/contrib/gperftools/lib/libtcmalloc_minimal.a CACHE INTERNAL "")
set (GPERFTOOLS_TCMALLOC_MINIMAL_DEBUG ${ClickHouse_BINARY_DIR}/contrib/gperftools/lib/libtcmalloc_minimal_debug.a CACHE INTERNAL "")
