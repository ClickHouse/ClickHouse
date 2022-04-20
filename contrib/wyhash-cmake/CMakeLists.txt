set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/wyhash)

add_library (wyhash "${LIBRARY_DIR}/wyhash.h")

target_include_directories(wyhash SYSTEM PUBLIC ${LIBRARY_DIR})

add_library (ch_contrib::wyhash ALIAS wyhash)
