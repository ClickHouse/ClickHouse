set(USEARCH_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/usearch")
set(USEARCH_SOURCE_DIR "${USEARCH_PROJECT_DIR}/include")

set(FP16_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/FP16")
set(ROBIN_MAP_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/robin-map")
set(SIMSIMD_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SimSIMD-map")

add_library(_usearch INTERFACE)

target_include_directories(_usearch SYSTEM INTERFACE
    ${FP16_PROJECT_DIR}/include
    ${ROBIN_MAP_PROJECT_DIR}/include
    ${SIMSIMD_PROJECT_DIR}/include
    ${USEARCH_SOURCE_DIR})

add_library(ch_contrib::usearch ALIAS _usearch)
target_compile_definitions(_usearch INTERFACE ENABLE_USEARCH)
