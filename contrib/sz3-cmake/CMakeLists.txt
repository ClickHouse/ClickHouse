option(ENABLE_SZ3 "Enable SZ3" ${ENABLE_LIBRARIES})

# This library is still under development, it writes its own version into the serialized compressed data.
# Therefore, update SZ3 with care to avoid breaking existing persistencies.

# Note sz3-cmake/SZ3/version.hpp which was statically generated from version.hpp.in, it must be updated when the library is updated.

if (NOT ENABLE_SZ3)
    message (STATUS "Not using SZ3")
    return()
endif()

set(SZ3_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/sz3")
set(SZ3_SOURCE_DIR "${SZ3_PROJECT_DIR}/include")
set(SZ3_VERSION_DIR "${ClickHouse_SOURCE_DIR}/contrib/sz3-cmake")

add_library(_sz3 INTERFACE)
target_include_directories(_sz3 SYSTEM INTERFACE ${SZ3_SOURCE_DIR})
target_include_directories(_sz3 SYSTEM INTERFACE ${SZ3_VERSION_DIR})

add_library(ch_contrib::sz3 ALIAS _sz3)
