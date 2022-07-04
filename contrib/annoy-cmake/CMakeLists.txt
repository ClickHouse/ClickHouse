set(ANNOY_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/annoy")
set(ANNOY_SOURCE_DIR "${ANNOY_PROJECT_DIR}/src")
set(ANNOY_INCLUDE_DIR "${ANNOY_PROJECT_DIR}/src")

add_library(_annoy ${ANNOY_SOURCE_DIR}/mman.h)
target_include_directories(_annoy SYSTEM PUBLIC ${ANNOY_INCLUDE_DIR})
set_target_properties(_annoy PROPERTIES LINKER_LANGUAGE CXX)

add_library(ch_contrib::annoy ALIAS _annoy)
