set(LIBNAME "PocoData")
set(POCO_LIBNAME "${LIBNAME}")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( SRCS ${HDRS_G})

if (NOT POCO_STATIC)
    add_definitions(-DTHREADSAFE)
endif (NOT POCO_STATIC)

if(MSVC AND NOT(MSVC_VERSION LESS 1400))
    set_source_files_properties(src/StatementImpl.cpp
        PROPERTIES COMPILE_FLAGS "/bigobj")
endif()

include_directories (BEFORE include)

add_library( "${LIBNAME}" ${LIB_MODE} ${SRCS} )
set_target_properties( "${LIBNAME}"
    PROPERTIES
    VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
    OUTPUT_NAME ${POCO_LIBNAME}
    DEFINE_SYMBOL Data_EXPORTS
    )

target_link_libraries( "${LIBNAME}" PocoFoundation)

if(POCO_ENABLE_DATA_MYSQL)
    find_package(MySQL)
    if(MYSQL_FOUND)
        include_directories("${MYSQL_INCLUDE_DIR}")
        message(STATUS "MySQL Support Enabled")
        add_subdirectory( MySQL )
    else()
        message(STATUS "MySQL Support Disabled - no MySQL library")
    endif(MYSQL_FOUND)
endif(POCO_ENABLE_DATA_MYSQL)

if(POCO_ENABLE_DATA_ODBC)
    find_package(ODBC)
    if(WIN32 AND NOT WINCE)
        set(ODBC_LIBRARIES "odbc32" "odbccp32")
        message(STATUS "Windows native ODBC Support Enabled")
        add_subdirectory( ODBC )
    else(WIN32 AND NOT WINCE)
        if(ODBC_FOUND)
            include_directories("${ODBC_INCLUDE_DIRECTORIES}")
            message(STATUS "ODBC Support Enabled")
            add_subdirectory( ODBC )
        else()
            message(STATUS "ODBC Support Disabled - no ODBC runtime")
        endif()
    endif(WIN32 AND NOT WINCE)
endif(POCO_ENABLE_DATA_ODBC)

if (POCO_ENABLE_TESTS)
    add_subdirectory(samples)
    add_subdirectory(testsuite)
endif ()
