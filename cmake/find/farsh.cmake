if (NOT ENABLE_FARSH)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/farsh/LICENSE")
    set (MISSING_INTERNAL_FARSH_LIBRARY 1)
    message (WARNING "submodule contrib/farsh is missing. to fix try run: \n git submodule update --init")
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/farsh")
    message (WARNING "submodule contrib/farsh is missing. to fix try run: \n git submodule update --init")
else()
    set (FARSH_LIBRARY farsh)
    set (USE_FARSH 1)
endif()

if (NOT USE_FARSH)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot enable farsh")
endif()
