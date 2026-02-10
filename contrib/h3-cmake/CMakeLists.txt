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
    "${H3_SOURCE_DIR}/lib/h3Index.c"
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

configure_file("${H3_SOURCE_DIR}/include/h3api.h.in" "${H3_BINARY_DIR}/include/h3api.h")

add_library(_h3 ${SRCS})
target_include_directories(_h3 SYSTEM PUBLIC "${H3_SOURCE_DIR}/include")
target_include_directories(_h3 SYSTEM PUBLIC "${H3_BINARY_DIR}/include")
target_compile_definitions(_h3 PRIVATE H3_HAVE_VLA)

add_library(ch_contrib::h3 ALIAS _h3)
