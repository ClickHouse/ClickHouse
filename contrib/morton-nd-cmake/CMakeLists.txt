add_library(_morton_nd INTERFACE)
target_include_directories(_morton_nd SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/morton-nd/include/")
add_library(ch_contrib::morton_nd ALIAS _morton_nd)
