set(SIMDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include")
set(SIMDJSON_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/src")
set(SIMDJSON_SRC "${SIMDJSON_SRC_DIR}/simdjson.cpp")

add_library(simdjson ${SIMDJSON_SRC})
target_include_directories(simdjson SYSTEM PUBLIC "${SIMDJSON_INCLUDE_DIR}" PRIVATE "${SIMDJSON_SRC_DIR}")
