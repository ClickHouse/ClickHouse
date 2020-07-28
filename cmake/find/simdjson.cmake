if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include/simdjson.h")
    message (WARNING "submodule contrib/simdjson is missing. to fix try run: \n git submodule update --init --recursive")
    return()
endif ()

option (USE_SIMDJSON "Use simdjson" ON)

message(STATUS "Using simdjson=${USE_SIMDJSON}")
