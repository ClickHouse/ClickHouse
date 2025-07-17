set(SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/SIMDCompressionAndIntersection)

set (SRCS
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/bitpacking.cpp"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/codecfactory.cpp"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/for.c"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/frameofreference.cpp"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/integratedbitpacking.cpp"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/intersection.cpp"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/simdbitpacking.cpp"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/simdintegratedbitpacking.cpp"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/simdpackedsearch.c"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/simdpackedselect.c"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/streamvbyte.c"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/usimdbitpacking.cpp"
    "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/src/varintdecode.c"
)

add_library(_simd_compression_and_intersection ${SRCS})

target_include_directories(_simd_compression_and_intersection SYSTEM BEFORE PUBLIC "${SIMD_COMPRESSION_INTERSECTION_LIBRARY_DIR}/include/")

add_library(ch_contrib::simd_compression_and_intersection ALIAS _simd_compression_and_intersection)

