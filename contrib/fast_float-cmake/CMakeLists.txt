add_library(fast_float INTERFACE)
target_include_directories(fast_float SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/fast_float/include/")
add_library(ch_contrib::fast_float ALIAS fast_float)
