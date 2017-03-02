set(LIBNAME "PocoNet")
set(POCO_LIBNAME "${LIBNAME}")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( SRCS ${HDRS_G})

# Windows and WindowsCE need additional libraries
if(WIN32)
    if(WINCE)
        set(SYSLIBS ${SYSLIBS} "ws2.lib" "iphlpapi.lib")
    else()
        set(SYSLIBS ${SYSLIBS} "ws2_32.lib" "iphlpapi.lib")
    endif()
else()
    include(cmake/test_anl.cmake)
    if (HAVE_GETADDRINFO_A)
       set(SYSLIBS ${SYSLIBS} "${ANL_LIB}")
    endif()
endif(WIN32)

include_directories (BEFORE include)

add_library( "${LIBNAME}" ${LIB_MODE} ${SRCS} )
set_target_properties( "${LIBNAME}"
    PROPERTIES
    VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
    OUTPUT_NAME ${POCO_LIBNAME}
    DEFINE_SYMBOL Net_EXPORTS
    )

target_link_libraries( "${LIBNAME}" PocoFoundation ${SYSLIBS})

if (POCO_ENABLE_TESTS)
    add_subdirectory(samples)
    add_subdirectory(testsuite)
endif ()

