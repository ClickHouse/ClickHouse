set(GPERFTOOLS_ROOT ${ClickHouse_SOURCE_DIR}/contrib/gperftools)
set(GPERFTOOLS_BUILD ${ClickHouse_BINARY_DIR}/contrib/gperftools)
include(ExternalProject)
ExternalProject_Add(
    gperftools
    PREFIX ${GPERFTOOLS_ROOT}
    #SOURCE_DIR ${GPERFTOOLS_ROOT}
    #CONFIGURE_COMMAND ./configure --enable-frame-pointers --prefix=<INSTALL_DIR>
    CONFIGURE_COMMAND autoreconf -i ${GPERFTOOLS_ROOT} && env CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${GPERFTOOLS_ROOT}/configure --enable-minimal --with-pic --prefix=${GPERFTOOLS_BUILD}
    #BUILD_IN_SOURCE 1
)
