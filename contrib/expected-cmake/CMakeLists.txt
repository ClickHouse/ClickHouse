add_library(expected INTERFACE)
target_include_directories(expected SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/expected/include")
add_library(ch_contrib::expected ALIAS expected)
