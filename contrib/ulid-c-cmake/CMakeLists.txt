option (ENABLE_ULID "Enable ulid" ${ENABLE_LIBRARIES})

if (NOT ENABLE_ULID)
    message(STATUS "Not using ulid")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/ulid-c")

set (SRCS
    "${LIBRARY_DIR}/src/ulid.c"
)

add_library(_ulid ${SRCS})
target_include_directories(_ulid SYSTEM PUBLIC "${LIBRARY_DIR}/include")
add_library(ch_contrib::ulid ALIAS _ulid)