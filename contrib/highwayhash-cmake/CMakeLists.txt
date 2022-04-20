SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/highwayhash")

add_library(highwayhash "${LIBRARY_DIR}/highwayhash/highwayhash_target.h"
                        "${LIBRARY_DIR}/highwayhash/highwayhash_target.cc"
                        "${LIBRARY_DIR}/highwayhash/instruction_sets.h"
                        "${LIBRARY_DIR}/highwayhash/instruction_sets.cc")

target_include_directories(highwayhash PUBLIC ${LIBRARY_DIR})

add_library(ch_contrib::highwayhash ALIAS highwayhash)
