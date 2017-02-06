set(LIBNAME "Poco_DataMySQL")
set(POCO_LIBNAME "${LIBNAME}")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( MYSQL_SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( MYSQL_SRCS ${HDRS_G})

add_definitions(-DTHREADSAFE -DNO_TCL)

add_library( "${LIBNAME}" ${LIB_MODE} ${MYSQL_SRCS} )
set_target_properties( "${LIBNAME}"
    PROPERTIES
    VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
    OUTPUT_NAME ${POCO_LIBNAME}
    DEFINE_SYMBOL MySQL_EXPORTS
    )

target_link_libraries( "${LIBNAME}" PocoFoundation PocoData ${MYSQL_LIB})

if (POCO_ENABLE_TESTS)
    add_subdirectory(testsuite)
endif ()
