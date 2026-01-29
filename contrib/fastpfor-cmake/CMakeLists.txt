option (ENABLE_FASTPFOR "Enable FastPFOR" ${ENABLE_LIBRARIES})

if (NOT ENABLE_FASTPFOR)
    message(STATUS "Not using FastPFOR")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/FastPFOR")

if (ARCH_AMD64)
    message(STATUS "FastPFOR: x86_64 detected")
elseif (ARCH_AARCH64)
    message(STATUS "FastPFOR: ARM64 detected, using SIMDE")
    if (NOT TARGET ch_contrib::simde)
        message(STATUS "FastPFOR: SIMDE not found, disabling")
        return()
    endif()
else()
    message(STATUS "FastPFOR: unsupported arch, disabling")
    return()
endif()

set (SRCS
    "${LIBRARY_DIR}/src/bitpacking.cpp"
    "${LIBRARY_DIR}/src/bitpackingaligned.cpp"
    "${LIBRARY_DIR}/src/bitpackingunaligned.cpp"
    "${LIBRARY_DIR}/src/horizontalbitpacking.cpp"
    "${LIBRARY_DIR}/src/simdunalignedbitpacking.cpp"
    "${LIBRARY_DIR}/src/codecfactory.cpp"
    "${LIBRARY_DIR}/src/simdbitpacking.cpp"
    "${LIBRARY_DIR}/src/varintdecode.c"
    "${LIBRARY_DIR}/src/streamvbyte.c"
)

add_library (_fastpfor ${SRCS})
target_include_directories (_fastpfor SYSTEM PUBLIC "${LIBRARY_DIR}/headers")

if (ARCH_AARCH64 AND TARGET ch_contrib::simde)
    target_link_libraries (_fastpfor PUBLIC ch_contrib::simde)
    # Enable native SSE type aliases (__m128i, etc.) in SIMDE
    target_compile_definitions (_fastpfor PUBLIC SIMDE_ENABLE_NATIVE_ALIASES SIMDE_X86_SSE_ENABLE_NATIVE_ALIASES SIMDE_X86_SSE2_ENABLE_NATIVE_ALIASES SIMDE_X86_SSE3_ENABLE_NATIVE_ALIASES SIMDE_X86_SSSE3_ENABLE_NATIVE_ALIASES SIMDE_X86_SSE4_1_ENABLE_NATIVE_ALIASES)
endif()

target_compile_options (_fastpfor PRIVATE
    -Wno-conversion -Wno-sign-conversion -Wno-old-style-cast
    -Wno-sign-compare -Wno-unused-parameter -Wno-implicit-fallthrough
)

set_target_properties (_fastpfor PROPERTIES
    CXX_STANDARD 11 CXX_STANDARD_REQUIRED ON POSITION_INDEPENDENT_CODE TRUE
)

add_library (ch_contrib::fastpfor ALIAS _fastpfor)
message(STATUS "Using FastPFOR")
