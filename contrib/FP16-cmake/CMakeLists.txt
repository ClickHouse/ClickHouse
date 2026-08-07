set (FP16_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/FP16/")

add_library(_fp16 INTERFACE)
target_include_directories(_fp16 SYSTEM INTERFACE ${FP16_PROJECT_DIR}/include)
