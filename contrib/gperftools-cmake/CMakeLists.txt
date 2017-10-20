set(GPERFTOOLS_ROOT ${ClickHouse_SOURCE_DIR}/contrib/gperftools)
set(GPERFTOOLS_BUILD ${ClickHouse_BINARY_DIR}/contrib/gperftools)
include(ExternalProject)
set(CC_FULL "${CMAKE_C_COMPILER_LAUNCHER} ${CMAKE_C_COMPILER}")
set(CXX_FULL "${CMAKE_CXX_COMPILER_LAUNCHER} ${CMAKE_CXX_COMPILER}")
get_property(C_LAUNCH GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
ExternalProject_Add(
    gperftools
    #PREFIX ${GPERFTOOLS_ROOT}
    DOWNLOAD_COMMAND ""
    #SOURCE_DIR ${GPERFTOOLS_ROOT}
    #CONFIGURE_COMMAND ./configure --enable-frame-pointers --prefix=<INSTALL_DIR>
    CONFIGURE_COMMAND autoreconf -i ${GPERFTOOLS_ROOT} && env "CC=${C_LAUNCH} ${CMAKE_C_COMPILER}" "CXX=${C_LAUNCH} ${CMAKE_CXX_COMPILER}" CFLAGS=${FULL_C_FLAGS} CXXFLAGS=${FULL_CXX_FLAGS} ${GPERFTOOLS_ROOT}/configure --enable-minimal --with-pic --enable-shared=no --prefix=${GPERFTOOLS_BUILD}
    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j `nproc || sysctl -n hw.ncpu || echo 4`
    #BUILD_IN_SOURCE 1
    #STEP_TARGETS build
)
# 

#add_library(tcmalloc_minimalm INTERFACE IMPORTED)
#add_library(tcmalloc_minimalm UNKNOWN IMPORTED)
#add_dependencies(tcmalloc_minimalm EP_gperftools)
#add_library(tcmalloc_minimalm STATIC IMPORTED)
#add_dependencies(tcmalloc_minimalm gperftools)
#set_target_properties(tcmalloc_minimalm PROPERTIES IMPORTED_LOCATION ${ClickHouse_BINARY_DIR}/contrib/gperftools/lib/libtcmalloc_minimal.a)
#set_target_properties(tcmalloc_minimalm PROPERTIES INCLUDE_DIRECTORIES ${GPERFTOOLS_INCLUDE_DIR})
#set_target_properties(tcmalloc_minimalm PROPERTIES INCLUDE_DIRECTORIES ${ClickHouse_BINARY_DIR}/contrib/gperftools/include)
#set_target_properties(tcmalloc_minimalm PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ClickHouse_BINARY_DIR}/contrib/gperftools/include)
#set_target_properties(tcmalloc_minimalm PROPERTIES INTERFACE_IMPORTED_LOCATION ${ClickHouse_BINARY_DIR}/contrib/gperftools/lib/libtcmalloc_minimal.a)
#set_target_properties(tcmalloc_minimalm PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ClickHouse_BINARY_DIR}/contrib/gperftools/include)
#target_include_directories(tcmalloc_minimalm INTERFACE BEFORE "${ClickHouse_BINARY_DIR}/contrib/gperftools/include")
#set (GPERFTOOLS_TCMALLOC_MINIMAL tcmalloc_minimalm CACHE INTERNAL "") 
set (GPERFTOOLS_TCMALLOC_MINIMAL ${ClickHouse_BINARY_DIR}/contrib/gperftools/lib/libtcmalloc_minimal.a CACHE INTERNAL "")

message(STATUS "cuuuuu target =${tcmalloc_minimalm} gpt=${gperftools} EP=${EP_gperftools} gpt=${GPERFTOOLS_TCMALLOC_MINIMAL}")
