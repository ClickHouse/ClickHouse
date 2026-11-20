set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/croaring")

set(SRCS
    "${LIBRARY_DIR}/src/array_util.c"
    "${LIBRARY_DIR}/src/bitset.c"
    "${LIBRARY_DIR}/src/bitset_util.c"
    "${LIBRARY_DIR}/src/isadetection.c"
    "${LIBRARY_DIR}/src/memory.c"
    "${LIBRARY_DIR}/src/roaring.c"
    "${LIBRARY_DIR}/src/roaring64.c"
    "${LIBRARY_DIR}/src/roaring_array.c"
    "${LIBRARY_DIR}/src/roaring_priority_queue.c"
    "${LIBRARY_DIR}/src/art/art.c"
    "${LIBRARY_DIR}/src/containers/array.c"
    "${LIBRARY_DIR}/src/containers/bitset.c"
    "${LIBRARY_DIR}/src/containers/containers.c"
    "${LIBRARY_DIR}/src/containers/convert.c"
    "${LIBRARY_DIR}/src/containers/mixed_andnot.c"
    "${LIBRARY_DIR}/src/containers/mixed_equal.c"
    "${LIBRARY_DIR}/src/containers/mixed_intersection.c"
    "${LIBRARY_DIR}/src/containers/mixed_negation.c"
    "${LIBRARY_DIR}/src/containers/mixed_subset.c"
    "${LIBRARY_DIR}/src/containers/mixed_union.c"
    "${LIBRARY_DIR}/src/containers/mixed_xor.c"
    "${LIBRARY_DIR}/src/containers/run.c")

add_library(_roaring ${SRCS})

# croaring expects bswap64() in portability.h from FreeBSD's byteswap.h,
# but there is only bswap_64 and __bswap64.
if (OS_FREEBSD)
    target_compile_definitions(_roaring PRIVATE bswap64=__bswap64)
endif()

# clang-21 MemorySanitizer checks the value instead of the shadow of the index operand of AVX-512
# `vpermi2var`/`vpermt2var` intrinsics (https://github.com/llvm/llvm-project/pull/148785, fixed in clang-22),
# which produces false positives in croaring's AVX-512 kernels (e.g. `avx512_emit_unique16`).
if (SANITIZE STREQUAL "memory")
    target_compile_definitions(_roaring PUBLIC CROARING_COMPILER_SUPPORTS_AVX512=0)
endif()

target_include_directories(_roaring PRIVATE "${LIBRARY_DIR}/include/roaring")
target_include_directories(_roaring SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include")
target_include_directories(_roaring SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/cpp")

target_link_libraries(_roaring PUBLIC clickhouse_common_io)

add_library(ch_contrib::roaring ALIAS _roaring)
