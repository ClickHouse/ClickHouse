option(ENABLE_SQLITE "Enable sqlite" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SQLITE)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/sqlite-amalgamation/sqlite3.c")
    message (WARNING "submodule contrib/sqlite3-amalgamation is missing. to fix try run: \n git submodule update --init")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal sqlite library")
    set (USE_SQLITE 0)
    return()
endif()

set (USE_SQLITE 1)
set (SQLITE_LIBRARY sqlite)
message (STATUS "Using sqlite=${USE_SQLITE}")
