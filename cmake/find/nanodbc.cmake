option(ENABLE_NANODBC "Enalbe nanodbc" ${ENABLE_LIBRARIES})

if (NOT ENABLE_NANODBC)
    set (USE_ODBC 0)
    return()
endif()

if (NOT ENABLE_ODBC)
    set (USE_NANODBC 0)
    message (STATUS "Using nanodbc=${USE_NANODBC}")
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/nanodbc/CMakeLists.txt")
    message (WARNING "submodule contrib/nanodbc is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal nanodbc library")
    set (USE_NANODBC 0)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/unixodbc/include")
    message (ERROR "submodule contrib/unixodbc is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal unixodbc needed for nanodbc")
    set (USE_NANODBC 0)
    return()
endif()

set (USE_NANODBC 1)

set (NANODBC_LIBRARY nanodbc)

set (NANODBC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/nanodbc/nanodbce")

message (STATUS "Using nanodbc=${USE_NANODBC}: ${NANODBC_INCLUDE_DIR} : ${NANODBC_LIBRARY}")
message (STATUS "Using unixodbc")
