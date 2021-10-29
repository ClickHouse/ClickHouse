if (NOT ENABLE_ODBC)
    return ()
endif ()

if (NOT USE_INTERNAL_NANODBC_LIBRARY)
    message (FATAL_ERROR "Only the bundled nanodbc library can be used")
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/nanodbc/CMakeLists.txt")
    message (FATAL_ERROR "submodule contrib/nanodbc is missing. to fix try run: \n git submodule update --init")
endif()

set (NANODBC_LIBRARY nanodbc)
set (NANODBC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/nanodbc/nanodbc")

message (STATUS "Using nanodbc: ${NANODBC_INCLUDE_DIR} : ${NANODBC_LIBRARY}")
