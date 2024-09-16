if (ARCH_AMD64)
    # Currently only x86, see comment in contrib/usearch-cmake/CMakeLists.txt
    set(SIMSIMD_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SimSIMD")
    set(SIMSIMD_SRCS ${SIMSIMD_PROJECT_DIR}/c/lib.c)
    add_library(_simsimd ${SIMSIMD_SRCS})
    target_include_directories(_simsimd SYSTEM PUBLIC "${SIMSIMD_PROJECT_DIR}/include")
    target_compile_definitions(_simsimd PUBLIC SIMSIMD_DYNAMIC_DISPATCH)
endif ()
