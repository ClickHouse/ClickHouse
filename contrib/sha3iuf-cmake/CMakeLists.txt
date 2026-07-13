set(SHA3IUF_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/SHA3IUF")
set(SHA3IUF_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/SHA3IUF")
set(SHA3IUF_SRC ${SHA3IUF_SRC_DIR}/sha3.c)

add_library(_sha3iuf ${SHA3IUF_SRC})
set_target_properties(_sha3iuf PROPERTIES C_STANDARD 99)
target_include_directories(_sha3iuf SYSTEM PUBLIC "${SHA3IUF_INCLUDE_DIR}")
add_library(ch_contrib::sha3iuf ALIAS _sha3iuf)
