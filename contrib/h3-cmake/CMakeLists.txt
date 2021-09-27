set(H3_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/h3/src/h3lib")
set(H3_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/h3/src/h3lib")

set(SRCS
"${H3_SOURCE_DIR}/lib/algos.c"
"${H3_SOURCE_DIR}/lib/baseCells.c"
"${H3_SOURCE_DIR}/lib/bbox.c"
"${H3_SOURCE_DIR}/lib/coordijk.c"
"${H3_SOURCE_DIR}/lib/faceijk.c"
"${H3_SOURCE_DIR}/lib/geoCoord.c"
"${H3_SOURCE_DIR}/lib/h3Index.c"
"${H3_SOURCE_DIR}/lib/h3UniEdge.c"
"${H3_SOURCE_DIR}/lib/linkedGeo.c"
"${H3_SOURCE_DIR}/lib/localij.c"
"${H3_SOURCE_DIR}/lib/mathExtensions.c"
"${H3_SOURCE_DIR}/lib/polygon.c"
"${H3_SOURCE_DIR}/lib/vec2d.c"
"${H3_SOURCE_DIR}/lib/vec3d.c"
"${H3_SOURCE_DIR}/lib/vertex.c"
"${H3_SOURCE_DIR}/lib/vertexGraph.c"
)

configure_file("${H3_SOURCE_DIR}/include/h3api.h.in" "${H3_BINARY_DIR}/include/h3api.h")

add_library(h3 ${SRCS})
target_include_directories(h3 SYSTEM PUBLIC "${H3_SOURCE_DIR}/include")
target_include_directories(h3 SYSTEM PUBLIC "${H3_BINARY_DIR}/include")
target_compile_definitions(h3 PRIVATE H3_HAVE_VLA)
if(M_LIBRARY)
    target_link_libraries(h3 PRIVATE ${M_LIBRARY})
endif()
