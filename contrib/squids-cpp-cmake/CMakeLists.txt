option(ENABLE_SQIDS "Enable sqids support" ${ENABLE_LIBRARIES})
if ((NOT ENABLE_SQIDS))
    message (STATUS "Not using sqids")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/sqids-cpp")

set (HDRS
    "${LIBRARY_DIR}/include/sqids/blocklist.hpp"
    "${LIBRARY_DIR}/include/sqids/sqids.hpp"
)

add_library(_sqids ${HDRS})
target_include_directories(_sqids SYSTEM PUBLIC "${LIBRARY_DIR}/include")

add_library(ch_contrib::squids ALIAS _sqids)
target_compile_definitions(_sqids INTERFACE ENABLE_SQIDS)
