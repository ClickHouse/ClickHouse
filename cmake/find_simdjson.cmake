if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include/simdjson/jsonparser.h")
    message (WARNING "submodule contrib/simdjson is missing. to fix try run: \n git submodule update --init --recursive")
    return()
endif ()

if (NOT HAVE_SSE42)
    message (WARNING "submodule contrib/simdjson requires support of SSE4.2 instructions")
    return()
elseif (NOT HAVE_PCLMULQDQ)
    message (WARNING "submodule contrib/simdjson requires support of PCLMULQDQ instructions")
    return()
endif ()

option (USE_SIMDJSON "Use simdjson" ON)
set (SIMDJSON_LIBRARY "simdjson")

message(STATUS "Using simdjson=${USE_SIMDJSON}: ${SIMDJSON_LIBRARY}")
