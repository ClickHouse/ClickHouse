set(LIBNAME "PocoDataODBC")
set(POCO_LIBNAME "${LIBNAME}")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( ODBC_SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( ODBC_SRCS ${HDRS_G})

add_definitions( ${ODBC_CFLAGS} -DTHREADSAFE)

add_library( "${LIBNAME}" ${LIB_MODE} ${ODBC_SRCS} )
set_target_properties( "${LIBNAME}"
    PROPERTIES
    VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
    OUTPUT_NAME ${POCO_LIBNAME}
    DEFINE_SYMBOL ODBC_EXPORTS
    )

target_link_libraries( "${LIBNAME}" PocoFoundation PocoData ${ODBC_LIBRARIES})

if (POCO_ENABLE_TESTS)
    add_subdirectory(testsuite)
endif ()
