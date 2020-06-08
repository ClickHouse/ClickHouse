set(SIMDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include")
set(SIMDJSON_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/src")
set(SIMDJSON_SRC
    ${SIMDJSON_SRC_DIR}/error.cpp
    ${SIMDJSON_SRC_DIR}/implementation.cpp
    ${SIMDJSON_SRC_DIR}/simdjson.cpp

    ${SIMDJSON_SRC_DIR}/arm64/implementation.cpp
    ${SIMDJSON_SRC_DIR}/arm64/dom_parser_implementation.cpp

    ${SIMDJSON_SRC_DIR}/fallback/implementation.cpp
    ${SIMDJSON_SRC_DIR}/fallback/dom_parser_implementation.cpp

    ${SIMDJSON_SRC_DIR}/haswell/implementation.cpp
    ${SIMDJSON_SRC_DIR}/haswell/dom_parser_implementation.cpp

    ${SIMDJSON_SRC_DIR}/westmere/implementation.cpp
    ${SIMDJSON_SRC_DIR}/westmere/dom_parser_implementation.cpp
)

add_library(${SIMDJSON_LIBRARY} ${SIMDJSON_SRC})
target_include_directories(${SIMDJSON_LIBRARY} SYSTEM PUBLIC "${SIMDJSON_INCLUDE_DIR}" PRIVATE "${SIMDJSON_SRC_DIR}")
