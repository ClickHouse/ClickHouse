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

# simdjson is using its own CPU dispatching and get confused if we enable AVX/AVX2 flags.
if(ARCH_AMD64)
    target_compile_options(_simdjson PRIVATE -mno-avx -mno-avx2)
endif()

# On ppc64le, ClickHouse globally defines __SSE2__ to enable SSE-to-AltiVec emulation.
# This causes simdjson's experimental json_string_builder to activate its SSE2 code path,
# which fails to compile due to __m128i vs __m128i_u type mismatch in the PPC SSE wrappers.
# Disable it since simdjson already has proper AltiVec/VMX support on ppc64.
if(ARCH_PPC64LE)
    target_compile_definitions(_simdjson PUBLIC SIMDJSON_EXPERIMENTAL_HAS_SSE2=0)
endif()

# We allow fallback implementations because binaries are built with all
# supported instructions, so one implementation will be selected and fallback
# will be disabled.
#
# However, ClickHouse may be run on CPUs that do not support all of these
# instructions. Since fallback is disabled by default in such cases, this will
# result in an UNSUPPORTED_ARCHITECTURE error.
target_compile_options(_simdjson PRIVATE -DSIMDJSON_IMPLEMENTATION_FALLBACK=1)

add_library(ch_contrib::simdjson ALIAS _simdjson)
