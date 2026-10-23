# GDeflate - a variant of DEFLATE whose bit stream is split into 32 interleaved sub-streams, so that a GPU can
# expand it in parallel, while the CPU compresses and expands it as fast as DEFLATE. It is the format of nvcomp's
# GDeflate and of DirectStorage.
# Upstream: https://github.com/NVIDIA/libdeflate (branch `gdeflate`), a fork of libdeflate.
#
# Only the parts of the fork that GDeflate needs are built. The fork exports the same symbols as libdeflate, which
# ClickHouse also links (contrib/libdeflate-cmake), so each of them is renamed by a header that every translation
# unit of the fork includes first, and that `include/gdeflate.h` includes for ClickHouse.

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/gdeflate")

# The architecture-specific code is pulled in via headers from these translation units, so the same source list
# builds on every target; the per-arch cpu_features.c files compile to nothing on a foreign architecture.
set(SRCS
    "${LIBRARY_DIR}/lib/utils.c"
    "${LIBRARY_DIR}/lib/arm/cpu_features.c"
    "${LIBRARY_DIR}/lib/x86/cpu_features.c"
    # For `libdeflate_deflate_compress_bound`, which the bound of a GDeflate page is computed from.
    "${LIBRARY_DIR}/lib/deflate_compress.c"
    "${LIBRARY_DIR}/lib/gdeflate_compress.c"
    "${LIBRARY_DIR}/lib/gdeflate_decompress.c"
)

add_library(_gdeflate ${SRCS})
add_library(ch_contrib::gdeflate ALIAS _gdeflate)

target_compile_options(_gdeflate PRIVATE -include "${CMAKE_CURRENT_SOURCE_DIR}/include/gdeflate_symbols.h")
# The whole library in GDeflate mode, `deflate_compress.c` included: `libdeflate_gdeflate_compress_bound` takes the bound
# of a page from its `libdeflate_deflate_compress_bound`, which only in this mode counts the packets the 32 streams end
# with. Without it the bound is 256 bytes short, and a page of incompressible bytes may not fit, or write past it.
target_compile_definitions(_gdeflate PRIVATE WITH_GDEFLATE=)
target_include_directories(_gdeflate PRIVATE "${LIBRARY_DIR}")
target_include_directories(_gdeflate SYSTEM BEFORE PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
