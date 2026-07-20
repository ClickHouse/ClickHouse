set(XSIMD_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/xsimd/include")

# xsimd is a header-only SIMD abstraction library. It is used by Arrow's vectorized codepaths
# (bit unpacking, byte stream split, UTF-8 validation).
add_library(_xsimd INTERFACE)
target_include_directories(_xsimd SYSTEM BEFORE INTERFACE ${XSIMD_INCLUDE_DIR})
add_library(ch_contrib::xsimd ALIAS _xsimd)
