option(USE_DENSITY "Enable DENSITY experimental compression library" ${ENABLE_LIBRARIES})

if (NOT USE_DENSITY)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/density/README.md")
    message (ERROR "submodule contrib/density is missing. to fix try run: \n git submodule update --init --recursive")
endif()
