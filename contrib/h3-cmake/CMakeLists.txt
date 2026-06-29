option (ENABLE_H3 "Enable H3" ${ENABLE_LIBRARIES})

if(NOT ENABLE_H3)
    message(STATUS "Not using H3")
    return()
endif()

set(H3_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/h3/src/h3lib")
set(H3_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/h3/src/h3lib")

set(SRCS
    "${H3_SOURCE_DIR}/lib/algos.c"
    "${H3_SOURCE_DIR}/lib/coordijk.c"
    "${H3_SOURCE_DIR}/lib/bbox.c"
    "${H3_SOURCE_DIR}/lib/polygon.c"
    "${H3_SOURCE_DIR}/lib/polyfill.c"
    "${H3_SOURCE_DIR}/lib/h3Index.c"
    "${H3_SOURCE_DIR}/lib/h3Assert.c"
    "${H3_SOURCE_DIR}/lib/vec2d.c"
    "${H3_SOURCE_DIR}/lib/vec3d.c"
    "${H3_SOURCE_DIR}/lib/vertex.c"
    "${H3_SOURCE_DIR}/lib/linkedGeo.c"
    "${H3_SOURCE_DIR}/lib/localij.c"
    "${H3_SOURCE_DIR}/lib/latLng.c"
    "${H3_SOURCE_DIR}/lib/directedEdge.c"
    "${H3_SOURCE_DIR}/lib/mathExtensions.c"
    "${H3_SOURCE_DIR}/lib/iterators.c"
    "${H3_SOURCE_DIR}/lib/vertexGraph.c"
    "${H3_SOURCE_DIR}/lib/faceijk.c"
    "${H3_SOURCE_DIR}/lib/baseCells.c"
)

file(READ "${ClickHouse_SOURCE_DIR}/contrib/h3/VERSION" H3_VERSION LIMIT_COUNT 1)
# Clean any newlines
string(REPLACE "\n" "" H3_VERSION "${H3_VERSION}")
string(REPLACE "." ";" H3_VERSION_LIST "${H3_VERSION}")
list(GET H3_VERSION_LIST 0 H3_VERSION_MAJOR)
list(GET H3_VERSION_LIST 1 H3_VERSION_MINOR)
list(GET H3_VERSION_LIST 2 H3_VERSION_PATCH)
configure_file("${H3_SOURCE_DIR}/include/h3api.h.in" "${H3_BINARY_DIR}/include/h3api.h")

add_library(_h3 ${SRCS})
target_include_directories(_h3 SYSTEM PUBLIC "${H3_SOURCE_DIR}/include")
target_include_directories(_h3 SYSTEM PUBLIC "${H3_BINARY_DIR}/include")
target_compile_definitions(_h3 PRIVATE H3_HAVE_VLA)

# h3 geo math (latLng/vec2d/vec3d/bbox/faceijk) is heavy on libm sqrt/sin/cos/atan2 and on
# per-cell geometry loops (cell area = sum of spherical-triangle areas, cell boundary = per-vertex
# coordinate transforms). Scope FP optimizations to this contrib only, overriding the global
# defaults whose cross-platform-determinism cost we accept here:
#  - -fno-math-errno: drop the per-call errno-guard branch+wrapper (no result change), and
#  - -fassociative-math + -fno-signed-zeros: together these let the vectorizer turn the per-vertex
#    geometry and the area reduction into packed SIMD (cellAreaRads2, _hex2dToGeo, _geoToHex2d).
#    Both are required - an FP reduction needs the `reassoc` and `nsz` fast-math flags as a pair, so
#    neither flag alone vectorizes anything. This is the main h3 speedup (e.g. h3CellAreaM2 -34%).
#  - -ffp-contract=fast: contract the remaining scalar/packed mul+add into FMA (overrides the global
#    -ffp-contract=off). Minor on its own for h3, kept for the FMA fusion in the vectorized loops.
target_compile_options(_h3 PRIVATE -fno-math-errno -ffp-contract=fast -fassociative-math -fno-signed-zeros)

add_library(ch_contrib::h3 ALIAS _h3)
