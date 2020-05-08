if (CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
    set (Unwind_ARCH "arm")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64")
    set (Unwind_ARCH "aarch64")
elseif (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR
        CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64" OR
        CMAKE_SYSTEM_PROCESSOR STREQUAL "corei7-64")
    set (Unwind_ARCH "x86_64")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^i.86$")
    set (Unwind_ARCH "x86")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^ppc64")
    set (Unwind_ARCH "ppc64")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^ppc")
    set (Unwind_ARCH "ppc32")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^mips")
    set (Unwind_ARCH "mips")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^hppa")
    set (Unwind_ARCH "hppa")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^ia64")
    set (Unwind_ARCH "ia64")
endif (CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")

set(LIBUNWIND_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libunwind)
set(LIBUNWIND_BINARY_DIR ${ClickHouse_BINARY_DIR}/contrib/libunwind)
set(LIBUNWIND_STATIC_LIBS ${LIBUNWIND_BINARY_DIR}/lib/libunwind.a ${LIBUNWIND_BINARY_DIR}/lib/libunwind-${Unwind_ARCH}.a)
set(LIBUNWIND_SHARED_LIBS ${LIBUNWIND_BINARY_DIR}/lib/libunwind.so ${LIBUNWIND_BINARY_DIR}/lib/libunwind-${Unwind_ARCH}.so)
set(LIBUNWIND_INCLUDE_DIR ${LIBUNWIND_BINARY_DIR}/include)
file(MAKE_DIRECTORY ${LIBUNWIND_INCLUDE_DIR})

include(ExternalProject)
ExternalProject_Add(
    libunwind
    PREFIX ${LIBUNWIND_BINARY_DIR}
    SOURCE_DIR ${LIBUNWIND_SOURCE_DIR}
    CONFIGURE_COMMAND ${LIBUNWIND_SOURCE_DIR}/autogen.sh CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} --enable-cxx-exceptions --enable-coredump --enable-ptrace --enable-setjmp --disable-minidebuginfo --disable-documentation --disable-tests --prefix=${LIBUNWIND_BINARY_DIR}
    BUILD_COMMAND make
    INSTALL_COMMAND make install
    BUILD_BYPRODUCTS ${LIBUNWIND_STATIC_LIBS} ${LIBUNWIND_SHARED_LIBS}
)

add_library (unwind INTERFACE IMPORTED)
add_dependencies(unwind libunwind)
set_property (TARGET unwind PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LIBUNWIND_INCLUDE_DIR})
if (MAKE_STATIC_LIBRARIES)
    set_property (TARGET unwind PROPERTY INTERFACE_LINK_LIBRARIES ${LIBUNWIND_STATIC_LIBS})
else ()
    set_property (TARGET unwind PROPERTY INTERFACE_LINK_LIBRARIES ${LIBUNWIND_SHARED_LIBS})
endif ()
set_property (TARGET unwind PROPERTY IMPORTED_CONFIGURATIONS RELEASE)

add_library(unwind-override unwind-override.c)
target_link_libraries(unwind-override PUBLIC unwind)

# TODO Do we need an install function here ?
