set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/t1ha")

add_library(t1ha ${LIBRARY_DIR}/t1ha.c ${LIBRARY_DIR}/t1ha.h)
target_include_directories(t1ha PUBLIC "${LIBRARY_DIR}")
add_library(ch_contrib::t1ha ALIAS t1ha)
