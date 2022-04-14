set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/farsh")

set(SRCS
        "${LIBRARY_DIR}/farsh.cpp"
        "${LIBRARY_DIR}/farsh.cpp"
        )

add_library(farsh ${SRCS})
