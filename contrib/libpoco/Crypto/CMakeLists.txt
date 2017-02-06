set(LIBNAME "PocoCrypto")
set(POCO_LIBNAME "${LIBNAME}")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( SRCS ${HDRS_G})

#add_definitions(-D_USRDLL)

include_directories (BEFORE include)

add_library( "${LIBNAME}" ${LIB_MODE} ${SRCS} )
set_target_properties( "${LIBNAME}"
    PROPERTIES
    VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
    OUTPUT_NAME ${POCO_LIBNAME}
    DEFINE_SYMBOL Crypto_EXPORTS
    )

target_link_libraries( "${LIBNAME}" PocoFoundation ${OPENSSL_LIBRARIES} )

if (POCO_ENABLE_TESTS)
    add_subdirectory(samples)
    add_subdirectory(testsuite)
endif ()
