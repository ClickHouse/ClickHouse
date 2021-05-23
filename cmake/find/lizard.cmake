option(USE_LIZARD "Enable Lizard experimental compression library" ${ENABLE_LIBRARIES})

if (NOT USE_LIZARD)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/lizard/README.md")
    message (ERROR "submodule contrib/lizard is missing. to fix try run: \n git submodule update --init --recursive")
endif()
