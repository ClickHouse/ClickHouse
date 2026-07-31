add_library(_ipnsort INTERFACE)
target_include_directories(_ipnsort SYSTEM BEFORE INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/ipnsort)
add_library(ch_contrib::ipnsort ALIAS _ipnsort)
