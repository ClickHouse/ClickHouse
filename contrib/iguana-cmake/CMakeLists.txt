set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/iguana/iguana")

# Build the portable code paths of the library (structural LZ compression + the rANS entropy
# coders). The SIMD/assembly fast paths, the command-line tool, the C bindings stub and the
# file-I/O helpers are not compiled here (see the submodule's CHANGES.ClickHouse.md).
set (SRCS
    "${LIBRARY_DIR}/common.cpp"
    "${LIBRARY_DIR}/error.cpp"
    "${LIBRARY_DIR}/entropy.cpp"
    "${LIBRARY_DIR}/output_stream.cpp"
    "${LIBRARY_DIR}/ans_bitstream.cpp"
    "${LIBRARY_DIR}/ans_byte_statistics.cpp"
    "${LIBRARY_DIR}/ans_nibble_statistics.cpp"
    "${LIBRARY_DIR}/ans1.cpp"
    "${LIBRARY_DIR}/ans32.cpp"
    "${LIBRARY_DIR}/ans_nibble.cpp"
    "${LIBRARY_DIR}/encoder.cpp"
    "${LIBRARY_DIR}/decoder.cpp"
)

# AVX-512 accelerated kernels are x86-64 only. They are dispatched at runtime via cpu_has_avx512(),
# so a binary built with them still runs on CPUs without AVX-512 (it uses the portable kernels).
if (ARCH_AMD64)
    list (APPEND SRCS "${LIBRARY_DIR}/ans32_avx512.cpp")
    set_source_files_properties ("${LIBRARY_DIR}/ans32_avx512.cpp"
        PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512bw -mavx512vl -mavx512dq")
endif ()

# NEON accelerated kernels are AArch64 only. NEON is part of the AArch64 baseline, so no extra
# compile flags are required; the kernels are selected at process start (see at_process_start).
if (ARCH_AARCH64)
    list (APPEND SRCS "${LIBRARY_DIR}/decoder_neon.cpp" "${LIBRARY_DIR}/ans32_neon.cpp")
endif ()

add_library (_iguana ${SRCS})

# Linked statically into ClickHouse, so no shared-object visibility/dllexport attributes.
target_compile_definitions (_iguana PUBLIC IGUANA_STATIC)

# ClickHouse only supports Clang (see cmake/tools.cmake); select the matching intrinsics in bitops.h.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_definitions (_iguana PRIVATE IGUANA_COMPILER_CLANG)
else ()
    target_compile_definitions (_iguana PRIVATE IGUANA_COMPILER_GNU)
endif ()

# The upstream port emits a number of -Wsign-compare / -Wmissing-field-initializers warnings.
target_compile_options (_iguana PRIVATE -w)

target_include_directories (_iguana SYSTEM BEFORE PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/iguana")

add_library (ch_contrib::iguana ALIAS _iguana)
