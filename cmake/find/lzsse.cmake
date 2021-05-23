option(USE_LZSSE "Enable LZSSE experimental compression library" ${ENABLE_LIBRARIES})

if (NOT USE_LZSSE)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/lzsse/README.md")
    message (ERROR "submodule contrib/lzsse is missing. to fix try run: \n git submodule update --init --recursive")
endif()
