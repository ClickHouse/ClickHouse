# See contrib/usearch-cmake/CMakeLists.txt, why only enabled on x86 and ARM
if (ARCH_AMD64 OR (ARCH_AARCH64 AND NOT NO_ARMV81_OR_HIGHER AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 19))
    set(SIMSIMD_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SimSIMD")
    set(SIMSIMD_SRCS ${SIMSIMD_PROJECT_DIR}/c/lib.c)
    add_library(_simsimd ${SIMSIMD_SRCS})
    target_include_directories(_simsimd SYSTEM PUBLIC "${SIMSIMD_PROJECT_DIR}/include")
    target_compile_definitions(_simsimd PUBLIC SIMSIMD_DYNAMIC_DISPATCH)
    add_library(ch_contrib::simsimd ALIAS _simsimd)
else()
    # The latter requires Clang 19 because older versions had a buggy bf16 implementation.
    message(STATUS "SimSIMD library is only supported on x86_64 with SSE or ARM with ARMv8.1 and Clang 19+")
endif()
