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
if (OS_WINDOWS)
    # `common/compiler_gcc.h` of the fork defines `_rotr64` and similar as macros unless they are already macros.
    # mingw-w64 declares them as functions in `stdlib.h`, which the fork includes only after that header (through
    # `immintrin.h`), so the macros break the declarations. Include `stdlib.h` first, so that its include guard
    # skips it later. `SHELL:` keeps CMake from merging the repeated `-include` with the one above.
    target_compile_options(_gdeflate PRIVATE "SHELL:-include stdlib.h")
endif ()
target_include_directories(_gdeflate PRIVATE "${LIBRARY_DIR}")
target_include_directories(_gdeflate SYSTEM BEFORE PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
