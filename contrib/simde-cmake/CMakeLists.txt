# SIMDE - SIMD Everywhere library
# Header-only library that provides portable SIMD intrinsics

option (ENABLE_SIMDE "Enable SIMDE (SIMD Everywhere) library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SIMDE)
    message(STATUS "Not using SIMDE")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/simde")

if (NOT EXISTS "${LIBRARY_DIR}/simde/simde-common.h")
    message(WARNING "SIMDE submodule not found, disabling")
    return()
endif()

add_library (_simde INTERFACE)

target_include_directories (_simde SYSTEM INTERFACE "${LIBRARY_DIR}")

add_library (ch_contrib::simde ALIAS _simde)

message(STATUS "Using SIMDE (SIMD Everywhere)")
