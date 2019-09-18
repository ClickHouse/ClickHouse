if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/fleece/README.md")
    message (WARNING "submodule contrib/fleece is missing. to fix try run: \n git submodule update --init --recursive")
    return()
endif ()

set (FLEECE_LIBRARY "fleece")
set (FLEECE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/fleece")

message(STATUS "Using fleece=${FLEECE_LIBRARY}: ${FLEECE_INCLUDE_DIR}")
