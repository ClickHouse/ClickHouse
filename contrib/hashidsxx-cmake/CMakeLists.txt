set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/hashidsxx")

set (SRCS
    "${LIBRARY_DIR}/hashids.cpp"
)

set (HDRS
    "${LIBRARY_DIR}/hashids.h"
)

add_library(_hashidsxx ${SRCS} ${HDRS})
target_include_directories(_hashidsxx SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::hashidsxx ALIAS _hashidsxx)
