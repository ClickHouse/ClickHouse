set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/farsh")

set(SRCS
        "${LIBRARY_DIR}/farsh.h"
        "${LIBRARY_DIR}/farsh.c"
        )

add_library(farsh ${SRCS})
add_library(ch_contrib::farsh ALIAS farsh)