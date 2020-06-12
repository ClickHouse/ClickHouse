set(SIMDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include")
set(SIMDJSON_SRC_DIR "${SIMDJSON_INCLUDE_DIR}/../src")
set(SIMDJSON_SRC
    ${SIMDJSON_SRC_DIR}/document.cpp
    ${SIMDJSON_SRC_DIR}/error.cpp
    ${SIMDJSON_SRC_DIR}/implementation.cpp
    ${SIMDJSON_SRC_DIR}/jsonioutil.cpp
    ${SIMDJSON_SRC_DIR}/jsonminifier.cpp
    ${SIMDJSON_SRC_DIR}/stage1_find_marks.cpp
    ${SIMDJSON_SRC_DIR}/stage2_build_tape.cpp
)

add_library(${SIMDJSON_LIBRARY} ${SIMDJSON_SRC})
target_include_directories(${SIMDJSON_LIBRARY} SYSTEM PUBLIC "${SIMDJSON_INCLUDE_DIR}" PRIVATE "${SIMDJSON_SRC_DIR}")
