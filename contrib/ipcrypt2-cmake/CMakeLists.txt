set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/ipcrypt2")

set (SRCS
    "${LIBRARY_DIR}/src/ipcrypt2.c"
)

add_library (_ipcrypt2 ${SRCS})
target_include_directories (_ipcrypt2 SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/src/include")

add_library (ch_contrib::ipcrypt2 ALIAS _ipcrypt2)
