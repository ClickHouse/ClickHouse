option (USE_SIMDJSON "Use simdjson" ${ENABLE_LIBRARIES})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include/simdjson.h")
    message (WARNING "submodule contrib/simdjson is missing. to fix try run: \n git submodule update --init --recursive")
    if (USE_SIMDJSON)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal simdjson library")
    endif()
    return()
endif ()

message(STATUS "Using simdjson=${USE_SIMDJSON}")
