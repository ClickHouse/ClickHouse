if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/rapidjson/include/rapidjson/rapidjson.h")
    message (WARNING "submodule contrib/rapidjson is missing. to fix try run: \n git submodule update --init --recursive")
    return()
endif ()

option (USE_RAPIDJSON "Use rapidjson" ON)
set (RAPIDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rapidjson/include")

message(STATUS "Using rapidjson=${USE_RAPIDJSON}: ${RAPIDJSON_INCLUDE_DIR}")
