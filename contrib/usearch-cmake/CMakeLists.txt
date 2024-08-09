set(FP16_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/FP16")
set(ROBIN_MAP_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/robin-map")
set(SIMSIMD_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SimSIMD")
set(USEARCH_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/usearch")

add_library(_usearch INTERFACE)

target_include_directories(_usearch SYSTEM INTERFACE
    ${FP16_PROJECT_DIR}/include
    ${ROBIN_MAP_PROJECT_DIR}/include
    ${SIMSIMD_PROJECT_DIR}/include
    ${USEARCH_PROJECT_DIR}/include)

add_library(ch_contrib::usearch ALIAS _usearch)
