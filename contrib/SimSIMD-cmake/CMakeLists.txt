set(SIMSIMD_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SimSIMD")

add_library(_simsimd INTERFACE)
target_include_directories(_simsimd SYSTEM INTERFACE "${SIMSIMD_PROJECT_DIR}/include")
