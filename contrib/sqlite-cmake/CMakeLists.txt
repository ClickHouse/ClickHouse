option(ENABLE_SQLITE "Enable sqlite" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SQLITE)
    message(STATUS "Not using sqlite")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/sqlite-amalgamation")

set(SRCS ${LIBRARY_DIR}/sqlite3.c)

add_library(_sqlite ${SRCS})
target_include_directories(_sqlite SYSTEM PUBLIC "${LIBRARY_DIR}")
add_library(ch_contrib::sqlite ALIAS _sqlite)
