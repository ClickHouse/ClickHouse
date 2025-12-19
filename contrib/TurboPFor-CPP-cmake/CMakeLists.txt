set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/TurboPFor-CPP")

# Core scalar sources - always needed (internal utilities used by both scalar and SIMD paths)
set(SRCS
    "${LIBRARY_DIR}/src/dispatch.cpp"
    "${LIBRARY_DIR}/src/scalar/p4enc32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4d1dec32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4_scalar_bitpack32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4_scalar_bitunpack32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4_scalar_bitunpackd1_32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4_scalar_internal.cpp"
)

# Determine which SIMD features to use based on ClickHouse's CPU feature options
# TurboPFor-CPP requires: SSE4.2 + POPCNT for 128v32, AVX2 for 256v32
set(TURBOPFOR_USE_SSE42 OFF)
set(TURBOPFOR_USE_AVX2 OFF)

if (ARCH_AMD64)
    if (ENABLE_SSE42 AND ENABLE_POPCNT)
        set(TURBOPFOR_USE_SSE42 ON)
    endif()
    if (ENABLE_AVX2 AND ENABLE_SSE42 AND ENABLE_POPCNT)
        set(TURBOPFOR_USE_AVX2 ON)
    endif()
endif()

# 128v32 functions: use SIMD (SSE4.2) or scalar fallback
if (TURBOPFOR_USE_SSE42)
    set(SSE42_SRCS
        "${LIBRARY_DIR}/src/simd/p4enc128v32.cpp"
        "${LIBRARY_DIR}/src/simd/p4d1dec128v32.cpp"
        "${LIBRARY_DIR}/src/simd/p4_simd_internal.cpp"
        "${LIBRARY_DIR}/src/simd/bitpack128v32_simd.cpp"
        "${LIBRARY_DIR}/src/simd/bitunpack128v32_simd.cpp"
    )
    list(APPEND SRCS ${SSE42_SRCS})
else()
    # Scalar fallback for 128v32
    list(APPEND SRCS
        "${LIBRARY_DIR}/src/scalar/bitpack128v32_scalar.cpp"
        "${LIBRARY_DIR}/src/scalar/p4enc128v32_scalar.cpp"
        "${LIBRARY_DIR}/src/scalar/p4d1dec128v32_scalar.cpp"
    )
endif()

# 256v32 functions: use SIMD (AVX2) or scalar fallback
if (TURBOPFOR_USE_AVX2)
    set(AVX2_SRCS
        "${LIBRARY_DIR}/src/simd/p4enc256v32.cpp"
        "${LIBRARY_DIR}/src/simd/p4d1dec256v32.cpp"
        "${LIBRARY_DIR}/src/simd/p4_simd_internal_256v.cpp"
        "${LIBRARY_DIR}/src/simd/bitpack256v32_simd.cpp"
        "${LIBRARY_DIR}/src/simd/bitunpack256v32_simd.cpp"
    )
    list(APPEND SRCS ${AVX2_SRCS})
else()
    # Scalar fallback for 256v32
    list(APPEND SRCS
        "${LIBRARY_DIR}/src/scalar/bitpack256v32_scalar.cpp"
        "${LIBRARY_DIR}/src/scalar/p4enc256v32_scalar.cpp"
        "${LIBRARY_DIR}/src/scalar/p4d1dec256v32_scalar.cpp"
    )
endif()

add_library(_turbopfor_cpp ${SRCS})

target_include_directories(_turbopfor_cpp SYSTEM PUBLIC "${LIBRARY_DIR}/include")
target_include_directories(_turbopfor_cpp PRIVATE "${LIBRARY_DIR}/src")

# Set preprocessor definitions and per-file compile flags for SIMD sources
if (TURBOPFOR_USE_SSE42)
    target_compile_definitions(_turbopfor_cpp PRIVATE ENABLE_SSE42)
endif()

if (TURBOPFOR_USE_AVX2)
    target_compile_definitions(_turbopfor_cpp PRIVATE ENABLE_AVX2)
endif()

add_library(ch_contrib::turbopfor_cpp ALIAS _turbopfor_cpp)
