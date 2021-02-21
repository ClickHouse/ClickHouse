SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/ryu)

add_library(ryu
${LIBRARY_DIR}/ryu/d2fixed.c
${LIBRARY_DIR}/ryu/d2s.c
${LIBRARY_DIR}/ryu/f2s.c
${LIBRARY_DIR}/ryu/generic_128.c
)

target_include_directories(ryu SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}")
