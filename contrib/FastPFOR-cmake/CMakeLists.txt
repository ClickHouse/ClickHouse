set(FASTPFOR_LIBRARY_DIR "${CMAKE_SOURCE_DIR}/contrib/FastPFOR")

set (SRCS
    "${FASTPFOR_LIBRARY_DIR}/src/bitpacking.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/bitpackingaligned.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/bitpackingunaligned.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/codecfactory.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/horizontalbitpacking.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/simdbitpacking.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/simdunalignedbitpacking.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/streamvbyte.c"
    "${FASTPFOR_LIBRARY_DIR}/src/varintdecode.c"
)

add_library(_fastpfor ${SRCS})

target_include_directories(_fastpfor SYSTEM BEFORE PUBLIC "${FASTPFOR_LIBRARY_DIR}/headers/")

if (ARCH_AARCH64)
    # TODO After reading the native build descriptions, I'm not convinced that SIMDe is really a necessary dependency on ARM. Let's try for now without it. Vectorization will likely not be the bottleneck

    # # SIMDe (SIMD everywhere) is a dependency of FastPFOR only on ARM machines.
    # set(SIMDE_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/simde)
    # add_library(_simde INTERFACE)
    # target_include_directories(_simde INTERFACE ${SIMDE_LIBRARY_DIR}/)
    # target_compile_definitions(_simde INTERFACE -DSIMDE_ENABLE_NATIVE_ALIASES -DSIMDE_ARM_NEON_A32V8_ENABLE -DSIMDE_ARM_NEON_A64V8_ENABLE)
    #
    # target_link_libraries(_fastpfor PRIVATE _simde)
    # target_include_directories(_fastpfor SYSTEM BEFORE PUBLIC ${SIMDE_LIBRARY_DIR}/)
endif()

add_library(ch_contrib::fastpfor ALIAS _fastpfor)
