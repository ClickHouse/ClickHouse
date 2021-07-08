set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/sqlite-amalgamation")

set(SRCS ${LIBRARY_DIR}/sqlite3.c)

add_library(sqlite ${SRCS})
target_include_directories(sqlite SYSTEM PUBLIC "${LIBRARY_DIR}")
