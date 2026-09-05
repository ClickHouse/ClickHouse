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

if (OS_WINDOWS)
    # croaring's headers define a handful of functions `inline`, and its `.c` files carry the C99
    # `extern inline` declarations that ask for one out-of-line copy of each, for callers that did
    # not inline them. In ELF that copy simply outranks the `linkonce_odr` copy a C++ caller emits
    # from the same header; in COFF the C++ copy is a COMDAT and the two are a duplicate
    # definition, which is a hard error at link time. C++ callers always have the inline definition
    # to fall back on, so the extra copy is not needed - but croaring is a submodule, so the
    # declarations are stripped into a generated copy of each affected source rather than edited
    # out in place.
    # Only the ones that actually collide: croaring's other `extern inline` copies are what its own
    # C sources call when they do not inline, so removing those breaks the build instead. Add a name
    # here if the linker reports it as a duplicate.
    set (DUPLICATED_INLINE_FUNCTIONS roaring_bitmap_contains)

    set (PATCHED_SRCS)
    foreach (src ${SRCS})
        file (READ "${src}" src_contents)
        set (patched_contents "${src_contents}")
        foreach (function_name ${DUPLICATED_INLINE_FUNCTIONS})
            string (REGEX REPLACE "extern inline[^;]*[^_a-zA-Z0-9]${function_name}\\([^;]*;" "" patched_contents "${patched_contents}")
        endforeach ()
        if (patched_contents STREQUAL src_contents)
            list (APPEND PATCHED_SRCS "${src}")
        else ()
            get_filename_component (src_name "${src}" NAME)
            set (patched "${CMAKE_CURRENT_BINARY_DIR}/no-extern-inline/${src_name}")
            file (WRITE "${patched}" "${patched_contents}")
            list (APPEND PATCHED_SRCS "${patched}")
        endif ()
    endforeach ()
    set (SRCS ${PATCHED_SRCS})
endif ()

add_library(_roaring ${SRCS})

# croaring expects bswap64() in portability.h from FreeBSD's byteswap.h,
# but there is only bswap_64 and __bswap64.
if (OS_FREEBSD)
    target_compile_definitions(_roaring PRIVATE bswap64=__bswap64)
endif()

target_include_directories(_roaring PRIVATE "${LIBRARY_DIR}/include/roaring")
target_include_directories(_roaring SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include")
target_include_directories(_roaring SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/cpp")

target_link_libraries(_roaring PUBLIC clickhouse_common_io)

add_library(ch_contrib::roaring ALIAS _roaring)
