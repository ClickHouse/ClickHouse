set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/hiredis")

set(SRCS
        "${LIBRARY_DIR}/alloc.c"
        "${LIBRARY_DIR}/async.c"
        "${LIBRARY_DIR}/dict.c"
        "${LIBRARY_DIR}/hiredis.c"
        "${LIBRARY_DIR}/net.c"
        "${LIBRARY_DIR}/read.c"
        "${LIBRARY_DIR}/sds.c"
        "${LIBRARY_DIR}/sockcompat.c"
        "${LIBRARY_DIR}/ssl.c"
        "${LIBRARY_DIR}/test.c"
        )

add_library(hiredis ${SRCS})

target_include_directories(hiredis SYSTEM PUBLIC "${SRC_DIR}/adapters")
target_include_directories(hiredis SYSTEM PUBLIC "${SRC_DIR}")