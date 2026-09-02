add_library(_fast_float INTERFACE)
target_include_directories(_fast_float SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/fast_float/include/")
add_library(ch_contrib::fast_float ALIAS _fast_float)
