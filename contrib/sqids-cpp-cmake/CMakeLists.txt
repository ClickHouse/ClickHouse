option(ENABLE_SQIDS "Enable sqids support" ${ENABLE_LIBRARIES})
if ((NOT ENABLE_SQIDS))
    message (STATUS "Not using sqids")
    return()
endif()

set (SQIDS_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/sqids-cpp")
set (SQIDS_INCLUDE_DIR "${SQIDS_SOURCE_DIR}/include")

add_library(_sqids INTERFACE)
target_include_directories(_sqids SYSTEM INTERFACE ${SQIDS_INCLUDE_DIR})

add_library(ch_contrib::sqids ALIAS _sqids)
target_compile_definitions(_sqids INTERFACE ENABLE_SQIDS)
