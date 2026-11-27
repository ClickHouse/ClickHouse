if (ARCH_AMD64 OR ARCH_AARCH64)
    option (ENABLE_HIGHWAY "Enable Google Highway, a SIMD library with runtime dispatch (used for vectorized math functions)" ${ENABLE_LIBRARIES})
elseif (ENABLE_HIGHWAY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Highway is used only on x86_64 and aarch64")
endif ()

if (NOT ENABLE_HIGHWAY)
    message (STATUS "Not using Highway")
    return ()
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/highway")

# Only the runtime part needed for dynamic dispatch (CPU detection and error reporting).
# The SIMD code itself is header-only and is compiled in the translation units that use it.
add_library (_highway
    "${LIBRARY_DIR}/hwy/abort.cc"
    "${LIBRARY_DIR}/hwy/targets.cc"
)

target_include_directories (_highway SYSTEM PUBLIC "${LIBRARY_DIR}")

# A static library: no symbol export attributes.
target_compile_definitions (_highway PUBLIC HWY_STATIC_DEFINE)

# Do not compile the variants that give nothing for the math functions over the AVX-512 target,
# to keep the binary size and the build time down. The dispatch selects AVX2 or AVX-512 on x86_64,
# and NEON or SVE variants on aarch64.
target_compile_definitions (_highway PUBLIC "HWY_DISABLED_TARGETS=(HWY_AVX3_DL|HWY_AVX3_ZEN4|HWY_AVX3_SPR|HWY_AVX10_2)")

add_library (ch_contrib::highway ALIAS _highway)
