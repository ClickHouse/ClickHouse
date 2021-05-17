if (NOT USE_INTERNAL_NANODBC_LIBRARY)
    return ()
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/nanodbc")

if (NOT TARGET unixodbc)
    message(FATAL_ERROR "Configuration error: unixodbc is not a target")
endif()

set (SRCS
    "${LIBRARY_DIR}/nanodbc/nanodbc.cpp"
)

add_library(nanodbc ${SRCS})

target_link_libraries (nanodbc PUBLIC unixodbc)
target_include_directories (nanodbc SYSTEM PUBLIC "${LIBRARY_DIR}/")
