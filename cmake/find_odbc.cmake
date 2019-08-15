# This file copied from contrib/poco/cmake/FindODBC.cmake to allow build without submodules

option (ENABLE_ODBC "Enable ODBC" ${OS_LINUX})
if(ENABLE_ODBC)
    if (OS_LINUX)
        option(USE_INTERNAL_ODBC_LIBRARY "Set to FALSE to use system odbc library instead of bundled" ${NOT_UNBUNDLED})
    else ()
        option(USE_INTERNAL_ODBC_LIBRARY "Set to FALSE to use system odbc library instead of bundled" OFF)
    endif()

    if(USE_INTERNAL_ODBC_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/unixodbc/README")
        message(WARNING "submodule contrib/unixodbc is missing. to fix try run: \n git submodule update --init --recursive")
        set(USE_INTERNAL_ODBC_LIBRARY 0)
        set(MISSING_INTERNAL_ODBC_LIBRARY 1)
    endif()

    set(ODBC_INCLUDE_DIRS ) # Include directories will be either used automatically by target_include_directories or set later.
    if(USE_INTERNAL_ODBC_LIBRARY AND NOT MISSING_INTERNAL_ODBC_LIBRARY)
        set(ODBC_LIBRARY unixodbc)
        set(ODBC_LIBRARIES ${ODBC_LIBRARY})
        set(ODBC_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/unixodbc/include")
        set(ODBC_FOUND 1)
    else()
        find_package(ODBC)
    endif ()

    if(ODBC_FOUND)
        set(USE_ODBC 1)
        set(ODBC_INCLUDE_DIRECTORIES ${ODBC_INCLUDE_DIRS}) # for old poco
        set(ODBC_INCLUDE_DIR ${ODBC_INCLUDE_DIRS}) # for old poco
    endif()

    message(STATUS "Using odbc=${USE_ODBC}: ${ODBC_INCLUDE_DIRS} : ${ODBC_LIBRARIES}")
endif()
