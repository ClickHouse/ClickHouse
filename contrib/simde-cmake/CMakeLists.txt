set(SIMDE_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/simde)
add_library(_simde INTERFACE)
target_include_directories(_simde INTERFACE ${SIMDE_LIBRARY_DIR}/)
target_compile_definitions(_simde INTERFACE -DSIMDE_ENABLE_NATIVE_ALIASES -DSIMDE_ARM_NEON_A32V8_ENABLE -DSIMDE_ARM_NEON_A64V8_ENABLE)
