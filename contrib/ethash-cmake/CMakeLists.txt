set(ETHASH_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/ethash/include")
set(ETHASH_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/ethash/lib")
set(ETHASH_SRC "${ETHASH_SRC_DIR}/keccak/keccak.c")

add_library(_ethash ${ETHASH_SRC})
target_include_directories(_ethash SYSTEM PUBLIC "${ETHASH_INCLUDE_DIR}" PRIVATE "${ETHASH_SRC_DIR}")
add_library(ch_contrib::ethash ALIAS _ethash)

