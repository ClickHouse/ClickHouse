if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include/simdjson/jsonparser.h")
    message (WARNING "submodule contrib/simdjson is missing. to fix try run: \n git submodule update --init --recursive")
    return()
endif ()

if (NOT HAVE_AVX2)
    message (WARNING "submodule contrib/simdjson requires AVX2 support")
    return()
endif ()

option (USE_SIMDJSON "Use simdjson" ON)

set (SIMDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include")
set (SIMDJSON_LIBRARY "simdjson")
