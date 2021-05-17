set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/sqlite")

set(SRCS ${LIBRARY_DIR}/sqlite3.c)

add_library(sqlite STATIC ${SRCS})
target_include_directories(sqlite SYSTEM PUBLIC "${LIBRARY_DIR}")
