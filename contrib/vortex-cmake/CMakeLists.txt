option (ENABLE_VORTEX "Enable the Vortex file format" ${ENABLE_LIBRARIES})

# Instrumented for MSan this library grows enough that linking `unit_tests_dbms` runs out of
# `R_X86_64_PC32` relocations - the binary no longer fits the 2 GiB the small code model allows.
if (SANITIZE STREQUAL "memory")
    set (ENABLE_VORTEX OFF)
endif()

# The same limit is hit by the fuzzers: `clickhouse_fuzzer` links the whole server instrumented for
# ASan and libFuzzer, and with this library it no longer fits the small code model either.
if (ENABLE_FUZZING)
    set (ENABLE_VORTEX OFF)
endif()

if (NOT ENABLE_VORTEX OR NOT TARGET ch_contrib::arrow)
    message(STATUS "Not using vortex")
    return()
endif()

clickhouse_add_rust_member(_ch_rust_vortex FEATURE vortex)
target_include_directories(_ch_rust_vortex INTERFACE include)
add_library(ch_rust::vortex ALIAS _ch_rust_vortex)
