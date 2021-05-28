if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/cld2")
    message (ERROR "submodule contrib/cld2 is missing. to fix try run: \n git submodule update --init --recursive")
endif()
