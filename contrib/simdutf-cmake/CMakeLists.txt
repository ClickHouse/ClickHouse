option (ENABLE_SIMDUTF "Use simdutf" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SIMDUTF)
    message(STATUS "Not using simdutf")
    return()
endif()

set(SIMDUTF_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdutf/include")
set(SIMDUTF_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdutf/src")
set(SIMDUTF_SRC "${SIMDUTF_SRC_DIR}/simdutf.cpp")

add_library(_simdutf ${SIMDUTF_SRC})
# The umbrella src/simdutf.cpp includes its per-architecture kernel headers
# (e.g. simdutf/icelake.h) from the src/ directory, so it must be on the
# private include path in addition to the public include/ directory.
target_include_directories(_simdutf SYSTEM PUBLIC "${SIMDUTF_INCLUDE_DIR}" PRIVATE "${SIMDUTF_SRC_DIR}")

# Like simdjson, simdutf performs its own runtime CPU dispatch. On x86-64-v2
# (and lower) globally-enabled AVX/AVX2 flags confuse the dispatcher, so disable
# them and let it pick the best implementation at runtime. For v3+, AVX2 is
# always available and simdutf can use its Haswell implementation directly.
if (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_LESS 3)
    target_compile_options(_simdutf PRIVATE -mno-avx -mno-avx2)
endif()

add_library(ch_contrib::simdutf ALIAS _simdutf)
