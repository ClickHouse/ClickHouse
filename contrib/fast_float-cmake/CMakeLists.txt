add_library(fast_float INTERFACE)
target_include_directories(fast_float INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/fast_float/include/")
