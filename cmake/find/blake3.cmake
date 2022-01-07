option(USE_BLAKE3 "Enable BLAKE3" ${ENABLE_LIBRARIES})

if (NOT USE_BLAKE3)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/BLAKE3/README.md")
    message (ERROR "submodule contrib/BLAKE3 is missing. to fix try run: \n git submodule update --init")
endif()
