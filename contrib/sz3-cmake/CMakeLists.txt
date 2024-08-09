option(ENABLE_SZ3 "Enable SZ3" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SZ3)
    message (STATUS "Not using SZ3")
    return()
endif()

set(SZ3_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/sz3")
set(SZ3_SOURCE_DIR "${SZ3_PROJECT_DIR}/include")

# TODO: generate into build directory
# TODO: don't generate trash into the result (just look at it)
configure_file(${SZ3_SOURCE_DIR}/SZ3/version.hpp.in ${SZ3_SOURCE_DIR}/SZ3/version.hpp)

add_library(_sz3 INTERFACE)
target_include_directories(_sz3 SYSTEM INTERFACE ${SZ3_SOURCE_DIR})

add_library(ch_contrib::sz3 ALIAS _sz3)
