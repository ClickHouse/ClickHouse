option (ENABLE_SIMDJSON "Use simdjson" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SIMDJSON)
    message(STATUS "Not using simdjson")
    return()
endif()

set(SIMDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/include")
set(SIMDJSON_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdjson/src")
set(SIMDJSON_SRC "${SIMDJSON_SRC_DIR}/simdjson.cpp")

add_library(_simdjson ${SIMDJSON_SRC})
target_include_directories(_simdjson SYSTEM PUBLIC "${SIMDJSON_INCLUDE_DIR}" PRIVATE "${SIMDJSON_SRC_DIR}")

# For x86-64-v2 (and lower), simdjson uses its own CPU dispatching and gets
# confused if we enable AVX/AVX2 flags. Disable them so it can pick the best
# implementation at runtime, and enable the scalar fallback for CPUs without
# AVX2.  For v3+, AVX2 is always available so simdjson can use its Haswell
# implementation directly with no dispatch overhead.
if (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_LESS 3)
    target_compile_options(_simdjson PRIVATE -mno-avx -mno-avx2)
endif()

# Enable the scalar fallback implementation. Binaries may run on CPUs that do
# not support all compiled-in SIMD paths; without the fallback, simdjson
# returns UNSUPPORTED_ARCHITECTURE.  For x86-64-v3+ the Haswell (AVX2)
# implementation is always available, so the fallback is unnecessary.
if (NOT (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 3))
    target_compile_options(_simdjson PRIVATE -DSIMDJSON_IMPLEMENTATION_FALLBACK=1)
endif()

# On LoongArch, simdjson's CPU dispatch reads the HWCAP_LOONGARCH_LSX / HWCAP_LOONGARCH_LASX
# bits (see contrib/simdjson/src/internal/isadetection.h). These are kernel UAPI macros defined
# in <asm/hwcap.h>, but simdjson only includes <sys/auxv.h>, which pulls in glibc's <bits/hwcap.h>.
# The glibc in our LoongArch sysroot is old enough that its <bits/hwcap.h> defines no bits for this
# architecture ("No bits defined for this architecture."), so the macros are unreachable and the
# build fails. Force-include the kernel header so the macros are always available. The header has an
# include guard and its values match glibc's, so this is a benign no-op once the sysroot is updated.
if(ARCH_LOONGARCH64)
    target_compile_options(_simdjson PRIVATE -include asm/hwcap.h)
endif()

# On ppc64le, ClickHouse globally defines __SSE2__ to enable SSE-to-AltiVec emulation.
# This causes simdjson's experimental json_string_builder to activate its SSE2 code path,
# which fails to compile due to __m128i vs __m128i_u type mismatch in the PPC SSE wrappers.
# Disable it since simdjson already has proper AltiVec/VMX support on ppc64.
if(ARCH_PPC64LE)
    target_compile_definitions(_simdjson PUBLIC SIMDJSON_EXPERIMENTAL_HAS_SSE2=0)
endif()

add_library(ch_contrib::simdjson ALIAS _simdjson)
