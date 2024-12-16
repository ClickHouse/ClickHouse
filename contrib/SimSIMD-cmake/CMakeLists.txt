# See contrib/usearch-cmake/CMakeLists.txt, why only enabled on x86
if (ARCH_AMD64)
    set(SIMSIMD_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SimSIMD")
    set(SIMSIMD_SRCS ${SIMSIMD_PROJECT_DIR}/c/lib.c)
    add_library(_simsimd ${SIMSIMD_SRCS})
    target_include_directories(_simsimd SYSTEM PUBLIC "${SIMSIMD_PROJECT_DIR}/include")
    target_compile_definitions(_simsimd PUBLIC SIMSIMD_DYNAMIC_DISPATCH)
endif()
