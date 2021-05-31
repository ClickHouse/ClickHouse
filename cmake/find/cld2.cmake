option(USE_CLD2 "Enable cld2" ${ENABLE_LIBRARIES})

if (NOT USE_CLD2)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/cld2")
    message (ERROR "submodule contrib/cld2 is missing. to fix try run: \n git submodule update --init --recursive")
endif()
